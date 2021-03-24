import psycopg2

from psycopg2.extensions import (
    PYDATE, PYDATEARRAY,
    PYDATETIME, PYDATETIMEARRAY,
    PYDATETIMETZ, PYDATETIMETZARRAY,
)

UNSAFE_TYPES = [
    (PYDATE, PYDATEARRAY),
    (PYDATETIME, PYDATETIMEARRAY),
    (PYDATETIMETZ, PYDATETIMETZARRAY),
]


class UnsafeTypeAdapter:
    """
    Adapter for psycopg typecasters that allows to receive data
    that is not supported in python e.g. BC dates (753-04-21 BC).
    """

    def __init__(
        self,
        original_type: type,
        original_array_type: type,
        safe_type: type = None
    ):
        self.original_type = original_type
        self.original_array_type = original_array_type

        self.safe_type = safe_type or psycopg2.STRING

        self.new_type = psycopg2.extensions.new_type(
            self.original_type.values,
            self.original_type.name,
            self.cast
        )
        self.new_array_type = psycopg2.extensions.new_array_type(
            self.original_array_type.values,
            self.original_array_type.name,
            self.new_type
        )

    def register(self, scope=None):
        """Registers new type with psycopg `register_type` function."""
        scope_types = (
            psycopg2.extensions.connection,
            psycopg2.extensions.cursor,
        )
        if not isinstance(scope, scope_types):
            scope = None
        psycopg2.extensions.register_type(self.new_type, scope)
        psycopg2.extensions.register_type(self.new_array_type, scope)

    def cast(self, value, cursor):
        """Tries to use original type otherwise returns value of safe type."""
        try:
            result = self.original_type(value, cursor)
        except ValueError:
            result = self.safe_type(value, cursor)

        return result


def register_types(scope=None):
    """Register adapters for unsafe psycopg types."""
    for unsafe_type, unsafe_array_type in UNSAFE_TYPES:
        adapter = UnsafeTypeAdapter(unsafe_type, unsafe_array_type)
        adapter.register(scope)
