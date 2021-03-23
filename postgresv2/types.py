import psycopg2

UNSAFE_TYPES = [
    psycopg2.extensions.PYDATE,
    psycopg2.extensions.PYDATETIME,
    psycopg2.extensions.PYDATETIMETZ,
]


class UnsafeTypeAdapter:
    """
    Adapter for psycopg typecasters that allows to receive data
    that is not supported in python e.g. BC dates (753-04-21 BC).
    """

    def __init__(self, original_type: type):
        self.original_type = original_type
        self.new_type = psycopg2.extensions.new_type(
            self.original_type.values,
            self.original_type.name,
            self.cast
        )

    def register(self, scope=None):
        """Registers new type with psycopg `register_type` function."""
        psycopg2.extensions.register_type(self.new_type, scope)

    def cast(self, value, cursor):
        """Tries to use original type otherwise returns string value."""
        try:
            result = self.original_type(value, cursor)
        except ValueError:
            result = value

        return result


def register_types(scope=None):
    """Register adapters for unsafe psycopg types."""
    for unsafe_type in UNSAFE_TYPES:
        adapter = UnsafeTypeAdapter(unsafe_type)
        adapter.register(scope)
