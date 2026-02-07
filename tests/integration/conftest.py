from typing import Iterator
import pytest
from sqlalchemy import create_engine, Engine
from tern.utils import create_now_str
from tests.test_environment import resolve_creator_uri
from tern.db_uitls import create_db, drop_db, list_databases
import dotenv


@pytest.fixture(autouse=True)
def load_dotenv():
    dotenv.load_dotenv()


@pytest.fixture()
def creator_engine() -> Engine:
    uri = resolve_creator_uri()
    return create_engine(uri, isolation_level="AUTOCOMMIT")


@pytest.fixture()
def test_db(creator_engine: Engine) -> Iterator[str]:
    now_str = create_now_str()
    db_name = "tern_test_" + now_str.lower()
    create_db(creator_engine, db_name=db_name)
    yield db_name
    drop_db(creator_engine, db_name=db_name)
    databases = list_databases(creator_engine)
    remaining = [db for db in databases if db.name == db_name]
    assert len(remaining) == 0
