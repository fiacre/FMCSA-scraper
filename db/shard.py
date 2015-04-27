import elasticsearch.exceptions
from sqlalchemy import event
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema
from pg_shards.config import Settings
from zope.sqlalchemy import ZopeTransactionExtension
from .database import Database
from .search import Search


class Shard(Session):
    search = Search(index='shards', doc_type='shard_lookup')

    @classmethod
    def create(cls, uuid, db_name=None, join_transaction=True):
        settings = Settings()

        if not db_name:
            db_name = settings.create_shards_on

        if cls.search.exists(id=uuid):
            raise ShardError("The following shard already exists: {0}".format(uuid))

        db = Database(db_name)
        conn = db.connect()

        try:
            conn.execute(CreateSchema(settings.schema_prefix + uuid))
        except ProgrammingError as E:
            raise ShardError("There was an error creating the schema.") from E
        finally:
            conn.close()

        # Add shard to search
        cls.search.index(id=uuid, body={'db_name': db_name})

        return cls(uuid, db_name, join_transaction=join_transaction)

    def __init__(self, uuid, db_name=None, join_transaction=True):
        self.uuid = uuid
        self.join_transaction = join_transaction
        self.prefix = Settings().schema_prefix
        self._session = None

        if db_name:
            self.db_name = db_name
        else:
            self.db_name = self.get_db_name()

        if join_transaction:
            super().__init__(bind=Database(self.db_name), extension=ZopeTransactionExtension())
        else:
            super().__init__(bind=Database(self.db_name))

        @event.listens_for(self, 'after_begin')
        def receive_before_flush(session, flush_context, instances):
            session.execute('SET search_path TO "{0}";'.format(session.prefix + session.uuid))

    def get_db_name(self):
        try:
            search_result = self.search.get(id=self.uuid)
            return search_result['_source']['db_name']
        except elasticsearch.exceptions.NotFoundError:
            raise ShardError("Specified shard `{0}` could not be found.".format(self.uuid))

    def __repr__(self):
        return "Shard(db_name={0}, schema={1})".format(self.db_name, self.uuid)

    @property
    def prefixed_uuid(self):
        return self.prefix + self.uuid


class ShardError(Exception):
    pass
