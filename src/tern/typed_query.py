from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    Mapping,
    Protocol,
    Sequence,
    TypeVar,
    overload,
)


class CursorProtocol(Protocol):
    @property
    def description(self) -> list[tuple[str, ...]] | None: ...

    def execute(
        self, query: str, params: Mapping[str, Any] | tuple[Any, ...] | None = None
    ) -> Any: ...

    def executemany(
        self, query: str, seq_of_params: Sequence[Mapping[str, Any] | tuple[Any, ...]]
    ) -> None: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]: ...

    def __iter__(self) -> Iterator[tuple[Any, ...]]: ...


class ToDict(Protocol):
    def to_dict(self) -> Mapping[str, Any]: ...


InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
CursorT = TypeVar("CursorT", bound=CursorProtocol)

Row = tuple[Any, ...] | Mapping[str, Any]


class TypedCursor(Generic[OutputT, CursorT]):
    def __init__(
        self,
        cursor: CursorT,
        output_type: type[OutputT],
        row_to_output: Callable[[Row], OutputT] | None = None,
    ) -> None:
        self._cursor = cursor
        self._output_type = output_type
        self._column_names: list[str] | None = None
        self._row_to_output = row_to_output

    @property
    def cursor(self) -> CursorProtocol:
        """Access the underlying DBAPI cursor."""
        return self._cursor

    def _get_column_names(self) -> list[str]:
        """Extract and cache column names from cursor description."""
        if self._column_names is None:
            if self._cursor.description is None:
                self._column_names = []
            else:
                self._column_names = [desc[0] for desc in self._cursor.description]
        return self._column_names

    def _convert_row(self, row: tuple[Any, ...]) -> OutputT:
        """Convert a single row tuple to an OutputT instance."""
        if self._row_to_output is not None:
            return self._row_to_output(row)
        column_names = self._get_column_names()
        row_dict = dict(zip(column_names, row))
        return self._output_type(**row_dict)

    def fetchone(self) -> OutputT | None:
        """Fetch the next row, returning a typed object or None."""
        row = self._cursor.fetchone()
        if row is None:
            return None
        return self._convert_row(row)

    def fetchall(self) -> list[OutputT]:
        """Fetch all remaining rows as typed objects."""
        rows = self._cursor.fetchall()
        return [self._convert_row(row) for row in rows]

    def fetchmany(self, size: int | None = None) -> list[OutputT]:
        """Fetch the next set of rows as typed objects."""
        rows = self._cursor.fetchmany(size)
        return [self._convert_row(row) for row in rows]

    def __iter__(self) -> Iterator[OutputT]:
        """Iterate over rows, yielding typed objects."""
        for row in self._cursor:
            yield self._convert_row(row)


class Query:
    def __init__(
        self,
        query: str,
    ) -> None:
        self._query = query

    @property
    def query(self) -> str:
        return self._query

    def execute(
        self,
        cursor: CursorT,
    ) -> CursorT:
        cursor.execute(self._query)
        return cursor

    def __call__(
        self,
        cursor: CursorProtocol,
    ) -> None:
        self.execute(cursor)


class ReturnQuery(Generic[OutputT]):
    def __init__(
        self,
        query: str,
        *,
        output_type: type[OutputT],
        row_to_output: Callable[[Row], OutputT] | None = None,
    ) -> None:
        self._query = query
        self._output_type = output_type
        self._row_to_output = row_to_output

    @property
    def query(self) -> str:
        return self._query

    def execute(
        self,
        cursor: CursorT,
    ) -> TypedCursor[OutputT, CursorT]:
        cursor.execute(self._query)
        return TypedCursor(cursor, self._output_type)

    def __call__(
        self,
        cursor: CursorProtocol,
    ) -> list[OutputT]:
        return self.execute(cursor).fetchall()


class ParametrizedQuery(Generic[InputT]):
    def __init__(self, query: str, *, input_type: type[InputT]) -> None:
        self._query = query
        self._input_type = input_type

    def params_to_dict(self, param: InputT) -> dict[str, Any] | tuple[Any, ...]:
        return param

    def execute(
        self,
        cursor: CursorT,
        params: InputT,
    ) -> CursorT:
        param_dict = self.params_to_dict(params)
        cursor.execute(self._query, param_dict)
        return cursor

    def executemany(
        self,
        cursor: CursorProtocol,
        seq_of_params: Sequence[InputT],
    ) -> None:
        cursor.executemany(
            self._query, [self.params_to_dict(params) for params in seq_of_params]
        )

    def __call__(
        self,
        cursor: CursorProtocol,
        params: InputT,
    ) -> None:
        self.execute(cursor, params)


class ParametrizedReturnQuery(Generic[InputT, OutputT]):
    def __init__(
        self,
        query: str,
        *,
        input_type: type[InputT],
        output_type: type[OutputT],
    ) -> None:
        self._query = query
        self._output_type = output_type
        self._input_type = input_type

    def params_to_dict(self, param: InputT) -> dict[str, Any] | tuple[Any, ...]:
        return {}

    def execute(
        self,
        cursor: CursorT,
        params: InputT,
    ) -> TypedCursor[OutputT, CursorT]:
        param_dict = self.params_to_dict(params)
        cursor.execute(self._query, param_dict)
        return TypedCursor(cursor, self._output_type)

    def executemany(
        self,
        cursor: CursorProtocol,
        seq_of_params: Sequence[InputT],
    ) -> None:
        cursor.executemany(
            self._query, [self.params_to_dict(params) for params in seq_of_params]
        )

    def __call__(
        self,
        cursor: CursorProtocol,
        params: InputT,
    ) -> list[OutputT]:
        return self.execute(cursor, params).fetchall()


@overload
def declare_query(
    query: str,
) -> Query: ...
@overload
def declare_query(
    query: str,
    *,
    input_type: type[InputT],
) -> ParametrizedQuery[InputT]: ...
@overload
def declare_query(
    query: str,
    *,
    output_type: type[OutputT],
) -> ReturnQuery[OutputT]: ...
@overload
def declare_query(
    query: str,
    *,
    input_type: type[InputT],
    output_type: type[OutputT],
) -> ParametrizedReturnQuery[InputT, OutputT]: ...
def declare_query(
    query: str,
    *,
    input_type: type[InputT] | None = None,
    output_type: type[OutputT] | None = None,
):
    if input_type is None:
        if output_type is None:
            return Query(query=query)
        else:
            return ReturnQuery(query=query, output_type=output_type)
    else:
        if output_type is None:
            return ParametrizedQuery(query=query, input_type=input_type)
        else:
            return ParametrizedReturnQuery(
                query=query, input_type=input_type, output_type=output_type
            )
