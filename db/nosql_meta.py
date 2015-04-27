from sqlalchemy.event import listen
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from .search import Search
from .nosql_property import NoSQLProperty


class NoSQLMeta(DeclarativeMeta):
    def __init__(cls, clsname, superclasses, attrs):
        super().__init__(clsname, superclasses, attrs)

        cls.__nosqlfields__ = {}
        cls.__search__ = {}

        # Track NoSQL and Search fields
        for attr_name in dir(cls):
            prop = getattr(cls, attr_name)

            if isinstance(prop, NoSQLProperty):
                prop.name = attr_name
                prop.name_validators()
                cls.__nosqlfields__[attr_name] = prop

            if isinstance(prop, Search):
                prop.name = attr_name
                cls.__search__[attr_name] = prop

        listen(cls, 'before_insert', NoSQLMeta.before_insert)
        listen(cls, 'before_update', NoSQLMeta.before_update)
        listen(cls, 'after_insert', NoSQLMeta.after_insert)
        listen(cls, 'after_update', NoSQLMeta.after_update)
        listen(cls, 'after_delete', NoSQLMeta.after_delete)

    def before_insert(mapper, connection, instance):
        try:
            instance.before_insert_event()
        except AttributeError:
            pass

        NoSQLMeta.apply_defaults(instance)
        NoSQLMeta.validate_nullable(instance)

    def before_update(mapper, connection, instance):
        try:
            instance.before_update_event()
        except AttributeError:
            pass

        NoSQLMeta.remove_deprecated_fields(instance)
        NoSQLMeta.validate_nullable(instance)
        NoSQLMeta.apply_onupdate(instance)

    def after_insert(mapper, connection, instance):
        NoSQLMeta.update_search(instance)

    def after_update(mapper, connection, instance):
        NoSQLMeta.update_search(instance)

    def after_delete(mapper, connection, instance):
        NoSQLMeta.remove_search(instance)

    def apply_defaults(instance):
        """Adds NoSQL default values to targets."""
        for field in instance.__nosqlfields__.values():
            field.apply_default(instance)

    def apply_onupdate(instance):
        """Adds NoSQL 'on update' values to targets."""
        for field in instance.__nosqlfields__.values():
            field.apply_on_update(instance)

    def validate_nullable(instance):
        """Ensures all required fields (nullable=False) are not None."""
        for field in instance.__nosqlfields__.values():
            field.validate_nullable(instance)

    def update_search(instance):
        for search in instance.__search__.values():
            search._index_item(instance)

    def remove_search(instance):
        for search in instance.__search__.values():
            search._delete_item(instance)

    def remove_deprecated_fields(instance):
        """Handle removing deprecated NoSQL fields from targets."""
        try:
            deprecated = instance.__deprecated__
        except AttributeError:
            return  # No deprecated fields defined

        for target_name, deprecated_fields in deprecated.items():
            target = getattr(instance, target_name)

            for ditem in deprecated_fields:
                try:
                    del target[ditem]
                except KeyError:
                    pass  # Field either already removed or never existed
