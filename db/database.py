from sqlalchemy import create_engine
from pg_shards.config import Settings


class Database:
    database_engines = {}

    def __new__(cls, db_name):
        if not cls.database_engines:
            cls.init_db_engines()

        try:
            return cls.database_engines[db_name]
        except KeyError:
            raise DatabaseError('The requested database `{0}` has not been configured.'.format(db_name))

    @classmethod
    def init_db_engines(cls):
        for db in Settings().databases:
            cls.database_engines[db.name] = create_engine(
                '{0.dialect}+{0.driver}://{0.username}:{0.password}@{0.host}:{0.port}/{0.name}'.format(db),
                echo=db.echo
            )


class DatabaseConfig:
    def __init__(self, name, dialect='postgresql', driver='psycopg2', host='localhost', port=5432,
                 username=None, password=None, echo=False):

        self.name = name
        self.dialect = dialect
        self.driver = driver
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.echo = echo


class DatabaseError(Exception):
    pass
