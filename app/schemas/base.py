from __future__ import annotations

from typing import Any, TypeVar

from .pydantic_compat import BaseModel, ConfigDict

T = TypeVar("T", bound="SchemaModel")


class SchemaModel(BaseModel):
    """Common base model with JSON helpers."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        use_enum_values=True,
        str_strip_whitespace=True,
    )

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_dict(cls: type[T], data: dict[str, Any]) -> T:
        return cls.model_validate(data)

    @classmethod
    def from_json(cls: type[T], payload: str) -> T:
        return cls.model_validate_json(payload)
