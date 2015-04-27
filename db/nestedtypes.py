

class NestedDict(dict):
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

    def clear(self):
        dict.clear(self)
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

    def changed(self):
        pass


class NestedList(list):

    def __getitem__(self, key):
        coerce_types = {dict: NestedDict, list: NestedList}
        value = list.__getitem__(self, key)

        for from_type, to_type in coerce_types.items():
            if not isinstance(value, to_type) and isinstance(value, from_type):
                value = to_type(value)
                value.changed = self.changed
                list.__setitem__(self, key, value)
                break

        return value

    def __setitem__(self, key, value):
        try:
            original_value = list.__getitem__(self, key)
        except KeyError:
            original_value = None

        list.__setitem__(self, key, value)  # Always set value as coerced mutable types test equal but must be set

        if original_value != value:
            self.changed()

    def __delitem__(self, key):
        list.__delitem__(self, key)
        self.changed()

    def append(self, value):
        list.append(self, value)
        self.changed()

    def clear(self):
        list.clear(self)
        self.changed()

    def extend(self, values):
        list.extend(self, values)
        self.changed()

    def insert(self, idx, value):
        list.insert(self, idx, value)
        self.changed()

    def pop(self, idx):
        result = list.pop(self, idx)
        self.changed()  # Mark changed only after successful pop
        return result

    def remove(self, value):
        list.remove(self, value)
        self.changed()

    def changed(self):
        pass
