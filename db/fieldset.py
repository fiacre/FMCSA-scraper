from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm.collections import mapped_collection
from pg_shards.db import JSONEncodedDict, NoSQLProperty, SequentialUUID
from pg_shards.libs import sequuid
from pg_shards.libs.validators import (
    ValidationError, 
    ValidBoolean, 
    ValidFloat, 
    ValidInteger, 
    ValidLength, 
    ValidMinMax,
    ValidNotEmpty, 
    ValidString
) 

class FieldSet:
    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    body = Column(JSONEncodedDict)
    polytype = Column(String(50))

    name = NoSQLProperty(target='body', validators=[ValidNotEmpty(), ValidString(), ValidLength(max_len=50)])
    description = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=500)])

    @declared_attr
    def __mapper_args__(cls):
        return {'polymorphic_on': cls.polytype, 'polymorphic_identity': 'fieldset'}

    @declared_attr
    def field_definitions(cls):
        return relationship(
            cls.__fieldset_definition__['class'],
            cascade="all, delete-orphan",
            collection_class=mapped_collection(lambda field_def: field_def.uuid),
            backref='fieldset'
        )

    def __init__(self, *args, **kwargs):
        super().__init__(uuid=sequuid(), *args, **kwargs)


class FieldSetAssociation:
    field_values = Column(JSONEncodedDict, default={})

    @declared_attr
    def owner_uuid(cls):
        return Column(SequentialUUID, ForeignKey(cls.__fieldset_owner__['table'] + '.uuid'), primary_key=True)

    @declared_attr
    def fieldset_uuid(cls):
        return Column(SequentialUUID, ForeignKey(cls.__fieldset__['table'] + '.uuid'), primary_key=True)

    @declared_attr
    def owner(cls):
        return relationship(
            cls.__fieldset_owner__['class'],
            backref=backref(
                'fieldsets',
                cascade="all, delete-orphan",
                collection_class=mapped_collection(lambda fieldset_assoc: fieldset_assoc.fieldset_uuid)
            )
        )

    @declared_attr
    def fieldset(cls):
        return relationship(
            cls.__fieldset__['class'],
            backref=backref(
                'fieldsets',
                cascade="all, delete-orphan"
            )
        )

    @declared_attr
    def field_definitions(cls):
        return association_proxy('fieldset', 'field_definitions')

    @declared_attr
    def name(cls):
        return association_proxy('fieldset', 'name')

    @declared_attr
    def description(cls):
        return association_proxy('fieldset', 'description')

    def __init__(self, fieldset):
        self.fieldset_uuid = fieldset.uuid

    @property
    def fields(self):
        try:
            return self._fields
        except AttributeError:
            self._fields = FieldsProxy(self.field_definitions, self.field_values)
            return self._fields

    def before_update_event(self):
        self.validate_required_fields()

    def validate_required_fields(self):
        self.fields.validate_required()


class FieldsProxy:
    def __init__(self, field_definitions, field_values):
        self.field_definitions = field_definitions
        self.field_values = field_values

    def __getitem__(self, key):
        try:
            return self.field_values[key]
        except KeyError:
            try:
                return self.field_definitions[key].default
            except KeyError:
                FieldSetError('The field for which you are trying to get a value does not exist.')

    def __setitem__(self, key, value):
        try:
            self.field_values[key] = self.field_definitions[key].validate(value)
        except KeyError:
            raise FieldSetError('The field definition for which this value is being set does not exist.')

    def __delitem__(self, key):
        try:
            del self.field_values[key]
        except KeyError:
            raise FieldSetError('Field value does not exist.')

    def __len__(self):
        return len(self.field_definitions)

    def __contains__(self, key):
        return key in self.field_definitions

    def __iter__(self):
        return self.keys()

    def keys(self):
        return self.field_definitions.keys()

    def values(self):
        for key in self.keys():
            yield self[key]

    def items(self):
        for key in self.keys():
            yield (key, self[key])

    def clear(self):
        self.field_values.clear()

    def validate_required(self):
        for field_key, field_def in self.field_definitions.items():
            if field_def.required:
                try:
                    ValidNotEmpty()(self.field_values[field_key])
                except (KeyError, ValidationError):
                    raise ValidationError("`{0}` must have a value.".format(field_def.name))


class FieldDefinition:
    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    body = Column(JSONEncodedDict)
    polytype = Column(String(50))

    name = NoSQLProperty(target='body', validators=[ValidNotEmpty(), ValidString(), ValidLength(max_len=50)])
    description = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=500)])
    required = NoSQLProperty(target='body', default=False, validators=[ValidBoolean()])
    default = NoSQLProperty(target='body', default=None)

    @declared_attr
    def __mapper_args__(cls):
        return {'polymorphic_on': cls.polytype, 'polymorphic_identity': 'field_def'}

    @declared_attr
    def fieldset_uuid(cls):
        return Column(SequentialUUID, ForeignKey(cls.__fieldset__['table'] + '.uuid'))

    def __init__(self, *args, **kwargs):
        super().__init__(uuid=sequuid(), *args, **kwargs)

    def validate(self, value, validators):
        if value is None and self.required:
            raise ValidationError("A value is required for {0}.".format(self.name))

        for validator in validators:
                value = validator(value)

        return value


class TextField(FieldDefinition):
    default = NoSQLProperty(
        target='body',
        validators=[ValidString()])

    min_len = NoSQLProperty(
        target='body',
        default=0,
        validators=[ValidInteger(), ValidMinMax(min_val=0, max_val=500)])

    max_len = NoSQLProperty(
        target='body',
        default=500,
        validators=[ValidInteger(), ValidMinMax(min_val=0, max_val=500)])

    @declared_attr
    def __mapper_args__(cls):
        return {'polymorphic_identity': 'text'}

    def validate(self, value):
        return super().validate(value, validators=[ValidString(),
                                                   ValidLength(min_len=self.min_len, max_len=self.max_len)])


class NumberField(FieldDefinition):
    default = NoSQLProperty(target='body', validators=[ValidInteger()])
    min_val = NoSQLProperty(target='body', default=0, validators=[ValidInteger()])
    max_val = NoSQLProperty(target='body', validators=[ValidInteger()])

    @declared_attr
    def __mapper_args__(cls):
        return {'polymorphic_identity': 'number'}

    def validate(self, value):
        return super().validate(value, validators=[ValidInteger(),
                                                   ValidMinMax(min_val=self.min_val, max_val=self.max_val)])


class DecimalField(FieldDefinition):
    default = NoSQLProperty(target='body', validators=[ValidFloat()])
    min_val = NoSQLProperty(target='body', default=0.0, validators=[ValidFloat()])
    max_val = NoSQLProperty(target='body', validators=[ValidFloat()])

    @declared_attr
    def __mapper_args__(cls):
        return {'polymorphic_identity': 'decimal'}

    def validate(self, value):
        return super().validate(value, validators=[ValidFloat(),
                                                   ValidMinMax(min_val=self.min_val, max_val=self.max_val)])


class FieldSetError(Exception):
    pass
