from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Callable, TypeVar, get_args, get_origin, get_type_hints

try:  # pragma: no cover - real dependency path
    from pydantic import BaseModel, ConfigDict, Field, model_validator  # type: ignore
except Exception:  # pragma: no cover - fallback path for this workspace
    T = TypeVar("T")

    @dataclass(slots=True)
    class _FieldInfo:
        default: Any = ...
        default_factory: Callable[[], Any] | None = None

    def Field(*, default: Any = ..., default_factory: Callable[[], Any] | None = None, **_: Any) -> Any:
        return _FieldInfo(default=default, default_factory=default_factory)

    class ConfigDict(dict):
        pass

    def model_validator(*, mode: str = "after") -> Callable[[T], T]:
        def decorator(func: T) -> T:
            setattr(func, "__model_validator_mode__", mode)
            return func

        return decorator

    class BaseModel:
        model_config: dict[str, Any] = {}

        def __init__(self, **data: Any):
            hints = {
                key: value
                for key, value in get_type_hints(self.__class__).items()
                if key != "model_config" and not key.startswith("_")
            }
            values: dict[str, Any] = {}
            remaining = dict(data)
            for name in hints:
                if name.startswith("_"):
                    continue
                if name in remaining:
                    values[name] = remaining.pop(name)
                    continue
                default = getattr(self.__class__, name, ...)
                if isinstance(default, _FieldInfo):
                    if default.default_factory is not None:
                        values[name] = self._coerce_value(default.default_factory(), hints.get(name))
                    elif default.default is not ...:
                        values[name] = self._coerce_value(default.default, hints.get(name))
                    else:
                        raise TypeError(f"Missing required field: {name}")
                elif default is not ...:
                    values[name] = self._coerce_value(default, hints.get(name))
                else:
                    raise TypeError(f"Missing required field: {name}")
            if remaining:
                extra = ", ".join(sorted(remaining))
                raise TypeError(f"Unexpected fields: {extra}")
            for key, value in values.items():
                setattr(self, key, self._coerce_value(value, hints.get(key)))
            self._run_model_validators()

        def _run_model_validators(self) -> None:
            for attr_name in dir(self.__class__):
                attr = getattr(self.__class__, attr_name)
                if callable(attr) and getattr(attr, "__model_validator_mode__", None) == "after":
                    result = attr(self)
                    if result is not None and result is not self:
                        self.__dict__.update(result.__dict__)

        @staticmethod
        def _coerce_value(value: Any, hint: Any) -> Any:
            if value is None or hint is None:
                return value
            origin = get_origin(hint)
            args = get_args(hint)
            if origin is list:
                item_hint = args[0] if args else Any
                return [BaseModel._coerce_value(item, item_hint) for item in value]
            if origin is dict:
                value_hint = args[1] if len(args) > 1 else Any
                return {key: BaseModel._coerce_value(item, value_hint) for key, item in value.items()}
            if origin is tuple:
                item_hint = args[0] if args else Any
                return tuple(BaseModel._coerce_value(item, item_hint) for item in value)
            if origin is not None and args:
                non_none = next((arg for arg in args if arg is not type(None)), None)  # noqa: E721
                if non_none is not None:
                    return BaseModel._coerce_value(value, non_none)
            if isinstance(hint, type):
                if issubclass(hint, BaseModel) and isinstance(value, dict):
                    return hint.model_validate(value)
                if issubclass(hint, Enum) and not isinstance(value, hint):
                    return hint(value)
                if hint is datetime and isinstance(value, str):
                    return datetime.fromisoformat(value)
            return value

        def model_dump(self, mode: str = "python") -> dict[str, Any]:
            return {
                key: self._serialize(value, mode=mode)
                for key, value in self.__dict__.items()
                if not key.startswith("_")
            }

        def model_dump_json(self) -> str:
            return json.dumps(self.model_dump(mode="json"), ensure_ascii=False, default=str)

        @classmethod
        def model_validate(cls, data: Any):
            if isinstance(data, cls):
                return data
            if not isinstance(data, dict):
                raise TypeError(f"Cannot validate type {type(data)!r} for {cls.__name__}")
            return cls(**data)

        @classmethod
        def model_validate_json(cls, payload: str):
            return cls.model_validate(json.loads(payload))

        def model_copy(self, update: dict[str, Any] | None = None):
            data = self.model_dump(mode="python")
            if update:
                data.update(update)
            return self.__class__(**data)

        @staticmethod
        def _serialize(value: Any, mode: str = "python") -> Any:
            if isinstance(value, BaseModel):
                return value.model_dump(mode=mode)
            if isinstance(value, Enum):
                return value.value
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, list):
                return [BaseModel._serialize(item, mode=mode) for item in value]
            if isinstance(value, tuple):
                return tuple(BaseModel._serialize(item, mode=mode) for item in value)
            if isinstance(value, dict):
                return {
                    key: BaseModel._serialize(item, mode=mode)
                    for key, item in value.items()
                }
            return value
