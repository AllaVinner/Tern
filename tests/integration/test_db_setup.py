from sqlalchemy import Engine
from tern.db_uitls import list_databases


def test_db_setup_and_teardown(test_db: str, creator_engine: Engine):
    databases = list_databases(creator_engine)
    remaining = [db for db in databases if db.name == test_db]
    assert len(remaining) == 1
