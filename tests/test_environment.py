import os
from tern.db_uitls import create_uri

PG_TEST_CREATOR_USERNAME_KEY = "TERN_PG_TEST_CREATOR_USERNAME"
PG_TEST_CREATOR_PASSWORD_KEY = "TERN_PG_TEST_CREATOR_PASSWORD"


def resolve_creator_uri(database: str | None = None) -> str:
    username = os.environ[PG_TEST_CREATOR_USERNAME_KEY]
    password = os.environ[PG_TEST_CREATOR_PASSWORD_KEY]
    uri = create_uri(username=username, password=password, database=database)
    return uri
