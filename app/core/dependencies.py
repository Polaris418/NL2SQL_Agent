def get_db_manager():
    from app.core.factory import get_container

    return get_container().db_manager


def get_agent():
    from app.core.factory import get_container

    return get_container().agent


def get_metadata_db():
    from app.core.factory import get_container

    return get_container().metadata_db


def get_service_container():
    from app.core.factory import get_container

    return get_container()


def get_llm_client():
    from app.core.factory import get_container

    return get_container().llm_client


def get_rag_index_manager():
    from app.core.factory import get_container

    return get_container().rag_index_manager
