from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.mutable import Mutable
from sqlalchemy.types import TypeDecorator
from .nestedtypes import NestedDict, NestedList


class JSONEncodedDict(TypeDecorator):
    impl = postgresql.JSON


class MutableJSONDict(Mutable, dict):
    def __getitem__(self, key):
        coerce_types = {dict: NestedDict, list: NestedList}
        value = dict.__getitem__(self, key)

        for from_type, to_type in coerce_types.items():
            if not isinstance(value, to_type) and type(value) == from_type:
                value = to_type(value)
                value.changed = self.changed
                dict.__setitem__(self, key, value)
                break

        return value

    def __setitem__(self, key, value):
        try:
            original_value = dict.__getitem__(self, key)
        except KeyError:
            original_value = None

        dict.__setitem__(self, key, value)  # Always set value as coerced mutable types test equal but must be set

        if original_value != value:
            self.changed()

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        self.changed()

    def update(self, *args, **kwargs):
        dict.update(self, *args, **kwargs)
        self.changed()

    def pop(self, *args, **kwargs):
        result = dict.pop(self, *args, **kwargs)
        self.changed()  # Mark changed only after successful pop
        return result

    def popitem(self):
        result = dict.popitem(self)
        self.changed()  # Mark changed only after successful pop
        return result

    def clear(self):
        dict.clear(self)
        self.changed()

    @classmethod
    def coerce(cls, key, value):
        if not isinstance(value, cls):
            if isinstance(value, dict):
                return cls(value)
            return Mutable.coerce(key, value)  # Raises ValueError for us
        else:
            return value


MutableJSONDict.associate_with(JSONEncodedDict)
