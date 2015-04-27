from copy import copy
from elasticsearch import Elasticsearch
from pg_shards.config import Settings


class Search:

    def __init__(self, index, doc_type, source_id=None, source=None, include=None, exclude=None,
                 mapping=None, read_only=False, instance_level=False):

        self.name = None  # This will be set to the attribute name by the metaclass
        self._es = None  # This will be set on first attribute access to avoid accessing settings at class level

        self._index_name = index
        self.doc_type = doc_type
        self.source_id = source_id
        self.source = source
        self.include = include
        self.exclude = exclude
        self.mapping = mapping
        self.read_only = read_only
        self.instance_level = instance_level

    def __get__(self, instance, instance_type=None):
        if self.instance_level and instance:
            # On instance level access return a Search object with the index name configured from instance attribute
            search = copy(self)
            search._index_name = getattr(instance, search._index_name)
            return search
        else:
            return self

    def __set__(self, instance, value):
        raise AttributeError('Search object cannot be overwritten.')

    def _index_item(self, instance):
        if self.read_only:
            return

        try:
            source_id = getattr(instance, self.source_id)
        except AttributeError:
            raise SearchError('Could not find the document ID using attribute name `{0}`'.format(self.source_id))

        try:
            source = getattr(instance, self.source)
            if callable(source):
                source = source()
        except AttributeError:
            raise SearchError('Could not find the document body using attribute name `{0}`'.format(self.source))

        if not source:
            return  # Nothing to index

        if self.include or self.exclude:
            source = source.copy()  # We're about to modify the source for indexing so make a copy

        if self.include:
            if isinstance(self.include, list):
                include = dict(zip(self.include, self.include))  # Turn list into dict of key:values (name:name)
            else:
                include = self.include

            for incl_key, incl_name in include.items():
                try:
                    incl_value = getattr(instance, incl_name)
                except AttributeError:
                    raise SearchError('Could not find attribute to be included: `{0}`'.format(incl_name))

                if callable(incl_value):
                    incl_value = incl_value()

                if incl_key in source:
                    raise SearchError("Included field collides with existing field.")

                source[incl_key] = incl_value

        if self.exclude:
            for excluded in self.exclude:
                try:
                    del source[excluded]
                except KeyError:
                    raise SearchError("Excluded field does not exist in source: `{0}`".format(excluded))

        if self.instance_level:
            getattr(instance, self.name).index(id=source_id, body=source)
        else:
            self.index(id=source_id, body=source)

    def _delete_item(self, instance):
        if self.read_only:
            return

        try:
            source_id = getattr(instance, self.source_id)
        except AttributeError:
            raise SearchError('Could not find the document ID using attribute name `{0}`'.format(self.source_id))

        if self.instance_level:
            getattr(instance, self.name).delete(id=source_id)
        else:
            self.delete(id=source_id)

    def _create_index(self, index=None):
        if self.read_only:
            return

        if self.instance_level:
            if index:
                index_name = Settings().search_index_prefix + index
            else:
                raise SearchError("Instance level indexes must be passed index argument on creation.")
        else:
            index_name = self.index_name

        if not self.indices.exists(index_name):
            self.indices.create(index=index_name)

            if self.mapping and not self.indices.exists_type(index=index_name, doc_type=self.doc_type):
                self.indices.put_mapping(index=index_name, doc_type=self.doc_type, body=self.mapping)

    def _delete_index(self):
        self.indices.delete(self.index_name)

    @property
    def es(self):
        if not self._es:
            self._es = ElasticSearch()
        return self._es

    @property
    def index_name(self):
        prefix = Settings().search_index_prefix

        if not self._index_name or self._index_name == '_all':
            # No index specified so search all of them with prefix
            return prefix + '*'
        else:
            return prefix + self._index_name

    @property
    def indices(self):
        return self.es.indices

    def index(self, *args, **kwargs):
        kwargs['index'] = self.index_name
        kwargs['doc_type'] = self.doc_type
        return self.es.index(*args, **kwargs)

    def exists(self, *args, **kwargs):
        kwargs['index'] = self.index_name

        if not kwargs.get('doc_type', None):
            kwargs['doc_type'] = self.doc_type

        return self.es.exists(*args, **kwargs)

    def get(self, *args, **kwargs):
        kwargs['index'] = self.index_name

        if not kwargs.get('doc_type', None):
            kwargs['doc_type'] = self.doc_type

        return self.es.get(*args, **kwargs)

    def get_source(self, *args, **kwargs):
        kwargs['index'] = self.index_name

        if not kwargs.get('doc_type', None):
            kwargs['doc_type'] = self.doc_type

        return self.es.get_source(*args, **kwargs)

    def delete(self, *args, **kwargs):
        kwargs['index'] = self.index_name

        if not kwargs.get('doc_type', None):
            kwargs['doc_type'] = self.doc_type

        return self.es.delete(*args, **kwargs)

    def search(self, *args, **kwargs):
        kwargs['index'] = self.index_name

        if not kwargs.get('doc_type', None):
            kwargs['doc_type'] = self.doc_type

        return self.es.search(*args, **kwargs)

    def _refresh(self, *args, **kwargs):
        kwargs['index'] = self.index_name
        return self.es.indices.refresh(*args, **kwargs)


class ElasticSearch:
    """
    Singleton class that configures and returns a global elastic search object.

    This class does not return itself but rather a configured instance of elasticsearch.Elasticsearch
    """
    _instance = None

    def __new__(cls, *args, **kwargs):

        if not cls._instance:
            # Create elastic search engine
            cls._instance = Elasticsearch(
                hosts=Settings().search_engines,
                sniff_on_start=True,
                sniff_on_connection_fail=True
            )

        return cls._instance


class SearchError(Exception):
    pass
