from tern.db_uitls import create_uri
from sqlalchemy import create_engine
from tern.typed_query import declare_query
from typing import TypedDict
from psycopg import connect


uri = create_uri(username="joel", password="joel", database="testdb")

engine = create_engine(uri)

create_database = declare_query("CREATE DATABASE playground")
create_schema = declare_query("CREATE SCHEMA tmp1")
create_table = declare_query("CREATE TABLE tmp1.user (id int, name varchar(20))")


class User(TypedDict):
    id: int
    name: str


get_users = declare_query("select * from tmp1.user", output_type=User)
insert_user = declare_query(
    "INSERT INTO tmp1.user (id, name) values (%(id)s, %(name)s)", input_type=User
)

if __name__ == "__main__":
    con = connect("user=joel password=joel dbname=playground", autocommit=True)

    cur = con.cursor()

    create_database(cur)
    create_schema(cur)
    create_table(cur)

    users = get_users(cur)
    insert_user(cur, User(id=2, name="Joel"))

    con.close()
