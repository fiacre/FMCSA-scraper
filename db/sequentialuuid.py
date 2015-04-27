from sqlalchemy.dialects import postgresql
from sqlalchemy.types import TypeDecorator
from pg_shards.libs import SeqUUID


class SequentialUUID(TypeDecorator):
    """
    Use Postgres' UUID implementation
    """
    impl = postgresql.UUID

    def process_bind_param(self, value, dialect):
        if value is None:
            return value

        return str(SeqUUID(value))  # Ensures we got a valid SeqUUID

    def process_result_value(self, value, dialect):
        if value is None:
            return value

        return SeqUUID(value).hex  # Removes dashes
