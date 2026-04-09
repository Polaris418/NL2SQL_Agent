import uvicorn

from app.core.factory import create_app, get_container


app = create_app()


def main() -> None:
    settings = get_container().settings
    uvicorn.run("app.main:app", host=settings.app_host, port=settings.app_port, reload=settings.debug)


if __name__ == "__main__":
    main()
