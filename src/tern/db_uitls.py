import psycopg
from psycopg import Connection
from tern.utils import print_tree
from dataclasses import dataclass, fields
from pydantic import BaseModel
from enum import StrEnum, auto
from typing import Callable, ParamSpec, TypeVar

Param = ParamSpec("Param")
RetType = TypeVar("RetType")


class BaseConfig(BaseModel):
    pass


class User(BaseConfig):
    username: str
    password: str


def query() -> Callable[[Callable[Param, RetType]], Callable[Param, RetType]]:
    def decorator(func: Callable[Param, RetType]) -> Callable[Param, RetType]:
        def dec_query(*args: Param.args, **kwargs: Param.kwargs) -> RetType:
            try:
                return func(*args, **kwargs)
            except psycopg.OperationalError:
                raise TernDBException(
                    "Could not connect to server with super user. Either there is a connection issue or the credentials are bad."
                )
            except psycopg.ProgrammingError as e:
                if len(e.args) > 0 and "InsufficientPrivilege" in e.args[0]:
                    raise TernDBException("insufficient privileges")
                raise

        return dec_query

    return decorator


def create_uri(
    username: str,
    password: str,
    driver: str | None = None,
    host: str | None = None,
    port: str | int | None = None,
    database: str | None = None,
):
    if driver is None:
        driver = "postgresql"
    if host is None:
        host = "localhost"
    if port is None:
        port = 5432
    if database is None:
        database = "postgres"
    uri = f"{driver}://{username}:{password}@{host}:{port}/{database}"
    return uri


def get_connection(
    database: str = "postgres", user: User | None = None, *, autocommit: bool = False
) -> Connection:
    if user is None:
        user = User(username="postgres", password="postgres")
    conninfo = f"postgresql://{user.username}:{user.password}@localhost:5432/{database}"
    return psycopg.connect(conninfo, autocommit=autocommit)


@dataclass
class TableListItem:
    table_catalog: str
    table_schema: str
    table_name: str
    table_type: str


def list_tables(conn: Connection) -> list[TableListItem]:
    column_names = [field.name for field in fields(TableListItem)]
    with conn.cursor() as cur:
        query = f"select {', '.join(column_names)} from information_schema.tables;"
        cur.execute(query)
        values = cur.fetchall()
    listing: list[TableListItem] = list()
    for row in values:
        kwargs = {name: value for name, value in zip(column_names, row)}
        item = TableListItem(**kwargs)
        listing.append(item)
    return listing


def print_tables(tables: list[TableListItem], *, include_pg: bool = False) -> None:
    paths = [
        f"{table.table_schema}/{table.table_name}"
        for table in tables
        if table.table_schema not in ["pg_catalog", "information_schema"]
    ]
    print_tree(paths)


@dataclass
class DatabaseListItem:
    name: str


def list_databases(conn: Connection) -> list[DatabaseListItem]:
    with conn.cursor() as cur:
        query = "select datname as name from pg_database;"
        cur.execute(query)
        values = cur.fetchall()
    listing: list[DatabaseListItem] = list()
    for row in values:
        item = DatabaseListItem(name=row[0])
        listing.append(item)
    return listing


@query()
def can_create_db(user: User) -> bool:
    conn = get_connection(user=user)
    with conn.cursor() as cur:
        stmt = f"select rolcreatedb from pg_roles where rolname = '{user.username}';"
        cur.execute(stmt)
        row = cur.fetchone()
        predicate = row[0] if row else None
    conn.close()
    return bool(predicate)


class TernException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class TernDBException(TernException): ...


class PGPolicy(StrEnum):
    CREATEDB = auto()


@query()
def create_user(
    super_user: User, target_user: User, policies: list[PGPolicy] | None = None
) -> None:
    if policies is None:
        policies = list()
    policy_str = "".join([policy + " " for policy in policies])
    conn = get_connection(user=super_user, autocommit=True)
    with conn.cursor() as cur:
        stmt = f"CREATE ROLE {target_user.username} {policy_str}LOGIN PASSWORD '{target_user.password}';"
        cur.execute(stmt)
    conn.close()


@query()
def grant_policies(
    super_user: User, target_user: User, policies: list[PGPolicy]
) -> None:
    conn = get_connection(user=super_user, autocommit=True)
    policy_str = "".join([policy + " " for policy in policies])
    stmt = f"ALTER USER {target_user.username} {policy_str}"
    with conn.cursor() as cur:
        cur.execute(stmt)
    conn.close()


class EnsureCreatorResult(StrEnum):
    inplace = auto()
    create_new_user = auto()
    grant_policy = auto()


class EnsureCreatorException(StrEnum):
    unable_to_connect = auto()
    no_super_user_provided = auto()
    super_user_unable_to_connect = auto()
    unable_to_crate_user = auto()
    unable_to_grant_policy = auto()


def ensure_creator(
    creator: User, super_user: User | None = None
) -> EnsureCreatorResult:
    try:
        creator_can_create = can_create_db(creator)
        creator_exists = True
    except TernDBException:
        creator_can_create = False
        creator_exists = False
    if creator_can_create:
        return EnsureCreatorResult.inplace
    if super_user is None:
        if creator_exists:
            msg = (
                "Creator user exists but is not granted the CREATEDB policy. "
                "No super user was provided. "
                "Please manually setup the user or supply a super user."
            )
        else:
            msg = (
                "Creator user exists does not exist or could not connect to database. "
                "No super user was provided. "
                "Please manually setup the user or supply a super user."
            )
        raise TernDBException(msg)
    if not creator_exists:
        create_user(
            super_user=super_user, target_user=creator, policies=[PGPolicy.CREATEDB]
        )
        return EnsureCreatorResult.create_new_user
    else:
        grant_policies(
            super_user=super_user, target_user=creator, policies=[PGPolicy.CREATEDB]
        )
        return EnsureCreatorResult.grant_policy


@query()
def create_db(conn: Connection, db_name: str):
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE {db_name}")


@query()
def drop_db(conn: Connection, db_name: str):
    with conn.cursor() as cur:
        cur.execute(f"DROP DATABASE {db_name}")
