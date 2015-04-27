from pg_shards.libs.validators import ValidationError


class NoSQLProperty:
    """
        A specialized property descriptor 
        that directs input/output to a target dictionary.
    """

    def __init__(self, fget=None, fset=None, fdel=None, doc=None,
                 target=None, default=None, onupdate=None, nullable=True,
                 validators=None, factories=None):

        assert target, "The `target` argument must be specified."

        # User defined getters, setters and deleters
        self.fget = fget
        self.fset = fset
        self.fdel = fdel

        self.name = None  # This will be set to the attribute name by the metaclass
        self.target = target
        self.default = default
        self.onupdate = onupdate
        self.nullable = nullable
        self.validators = validators
        self.factories = factories

        if doc is None and fget is not None:
            doc = fget.__doc__

        self.__doc__ = doc

    def __get__(self, instance, instance_type=None, for_setter=False):
        if instance is None:
            return self

        self.assert_target(instance)

        try:
            value = getattr(instance, self.target)[self.name]
        except KeyError:
            self.apply_default(instance)
            value = getattr(instance, self.target)[self.name]  # Ensures that coercion takes place for mutables

        if self.factories:
            for factory in self.factories:
                value = factory.value_out(value, instance, self.name, self.target)

        if self.fget and not for_setter:
            value = self.fget(instance, value)

        return value

    def __set__(self, instance, value):
        self.assert_target(instance)

        if self.validators:
            for validator in self.validators:
                value = validator(value)

        if self.fset:
            value = self.fset(instance, self.__get__(instance, for_setter=True), value)

        if self.factories:
            for factory in self.factories:
                value = factory.value_in(value, instance, self.name, self.target)

        getattr(instance, self.target)[self.name] = value

    def apply_default(self, instance):
        self.assert_target(instance)

        try:
            getattr(instance, self.target)[self.name]
        except KeyError:
            value = self.default() if callable(self.default) else self.default

            if self.validators:
                for validator in self.validators:
                    value = validator(value)

            if self.factories:
                for factory in self.factories:
                    value = factory.value_in(value, instance, self.name, self.target)

            getattr(instance, self.target)[self.name] = value

    def assert_target(self, instance):
        try:
            if getattr(instance, self.target) is None:
                setattr(instance, self.target, {})
        except AttributeError:
            raise NoSQLPropertyError('Cannot set attribute, target field `{0}` does not exist'.format(self.target))

    def apply_on_update(self, instance):
        if self.onupdate is not None:
            setattr(instance, self.name, self.onupdate() if callable(self.onupdate) else self.onupdate)

    def name_validators(self):
        if self.validators:
            for validator in self.validators:
                if not validator.field_name:
                    validator.field_name = self.name

    def validate_nullable(self, instance):
        if not self.nullable and getattr(instance, self.name) is None:
            E = ValidationError("The `{0}` field cannot be None.".format(self.name))
            E.field_name = self.name
            raise E

    def nosql_getter(self, fget):
        return type(self)(fget, self.fset, self.fdel, self.__doc__,
                          self.target, self.default, self.onupdate, self.nullable,
                          self.validators, self.factories)

    def nosql_setter(self, fset):
        return type(self)(self.fget, fset, self.fdel, self.__doc__,
                          self.target, self.default, self.onupdate, self.nullable,
                          self.validators, self.factories)


class NoSQLPropertyError(Exception):
    pass
