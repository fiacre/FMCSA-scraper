import dateutil.parser
from uuid import UUID
from pg_shards.libs import SeqUUID
import pytz


class Factory:
    def value_in(self, value, instance=None, prop_name=None, target=None):
        return value

    def value_out(self, value, instance=None, prop_name=None, target=None):
        return value


class Delegate(Factory):
    def __init__(self, factory):
        self.factory = factory

    def value_in(self, value, instance=None, prop_name=None, target=None):
        return self.factory.__setnosql__(value, instance, prop_name, target)

    def value_out(self, value, instance=None, prop_name=None, target=None):
        return self.factory.__getnosql__(value, instance, prop_name, target)


class DateTimeFactory(Factory):
    def value_in(self, value, instance=None, prop_name=None, target=None):
        if value is None:
            return value

        return value.isoformat()

    def value_out(self, value, instance=None, prop_name=None, target=None):
        if value is None:
            return value

        return dateutil.parser.parse(value)


class SeqUUIDFactory(Factory):
    def __init__(self, hex_out=True):
        self.hex_out = hex_out

    def value_in(self, value, instance=None, prop_name=None, target=None):
        if value is None:
            return value

        return value.hex

    def value_out(self, value, instance=None, prop_name=None, target=None):
        if value is None:
            return value

        if self.hex_out:
            return value
        else:
            return SeqUUID(value)


class UUIDFactory(Factory):
    def __init__(self, hex_out=True):
        self.hex_out = hex_out

    def value_in(self, value, instance=None, prop_name=None, target=None):
        if value is None:
            return value

        return value.hex

    def value_out(self, value, instance=None, prop_name=None, target=None):
        if value is None:
            return value

        if self.hex_out:
            return value
        else:
            return UUID(hex=value)


class TimezoneFactory(Factory):
    def value_in(self, value, instance=None, prop_name=None, target=None):
        if value is None:
            return value

        return value.zone

    def value_out(self, value, instance=None, prop_name=None, target=None):
        if value is None:
            return value

        return pytz.timezone(value)
