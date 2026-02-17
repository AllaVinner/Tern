from psycopg import Connection
from tern.db_uitls import list_databases


def test_db_setup_and_teardown(test_db: str, creator_connection: Connection):
    databases = list_databases(creator_connection)
    remaining = [db for db in databases if db.name == test_db]
    assert len(remaining) == 1
