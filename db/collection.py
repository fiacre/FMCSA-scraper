from sqlalchemy.orm.collections import collection
import weakref


class CollectionBase:

    @collection.linker
    def linker(self, adapter):
        self._parent = weakref.ref(adapter.owner_state.object)

    @property
    def parent(self):
        return self._parent()

    @property
    def shard(self):
        return self.parent.shard


class ListCollection(CollectionBase, list):
    pass


class DictCollection(CollectionBase, dict):
    pass
