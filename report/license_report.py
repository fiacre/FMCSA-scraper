import re
import dateutil
from sqlalchemy import Column, String, Integer
from sqlalchemy.schema import UniqueConstraint
from scraper.db.factories import DateTimeFactory
from scraper.db import JSONEncodedDict, NoSQLMeta, NoSQLProperty, SequentialUUID
from scraper.libs import sequuid
from scraper.libs.validators import ValidLength, ValidMinMax, ValidBoolean, ValidString
from scraper.models.fmcsa.validators import (
    ValidFMCSAFloat,
    ValidFMCSAInteger,
    ValidFMCSAString,
    ValidFMCSAMoney
)
from scraper.models import BaseModel, CUDFields
from scraper.models.fmcsa.validators import ValidFMCSADate


class LicenseReport(CUDFields, BaseModel, metaclass=NoSQLMeta):
    """
        main license page
        a versioned report
    """
    __tablename__ = 'license'
    __table_args__ = (UniqueConstraint('dot_number', 'version', name='lic_version_dot_uc'),)

    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    dot_number = Column(String(10))
    version = Column(Integer)
    body = Column(JSONEncodedDict)

    common_authority_status = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=16)])
    contract_authority_status = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=16)])
    broker_authority_status = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=16)])

    common_application_pending = NoSQLProperty(target='body', validators=[ValidBoolean()])
    contract_application_pending = NoSQLProperty(target='body', validators=[ValidBoolean()])
    broker_application_pending = NoSQLProperty(target='body', validators=[ValidBoolean()])

    ins_property = NoSQLProperty(target='body', validators=[ValidBoolean()])
    ins_passenger = NoSQLProperty(target='body', validators=[ValidBoolean()])
    ins_household_goods = NoSQLProperty(target='body', validators=[ValidBoolean()])
    ins_private = NoSQLProperty(target='body', validators=[ValidBoolean()])
    ins_enterprise = NoSQLProperty(target='body', validators=[ValidBoolean()])

    bipd_required = NoSQLProperty(target='body', validators=[ValidFMCSAMoney(), ValidLength(max_len=128)])
    bipd_on_file = NoSQLProperty(target='body', validators=[ValidFMCSAMoney(), ValidLength(max_len=128)])
    cargo_required = NoSQLProperty(target='body', validators=[ValidBoolean()])
    cargo_on_file = NoSQLProperty(target='body', validators=[ValidBoolean()])
    bond_required = NoSQLProperty(target='body', validators=[ValidBoolean()])
    bond_on_file = NoSQLProperty(target='body', validators=[ValidBoolean()])

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        if self.common_authority_status and other.common_authority_status:
            if self.common_authority_status != other.common_authority_status:
                return False
        if self.contract_authority_status and other.contract_authority_status:
            if self.contract_authority_status != other.contract_authority_status:
                return False
        if self.broker_authority_status and other.broker_authority_status:
            if self.broker_authority_status != other.broker_authority_status:
                return False
        if self.common_application_pending and other.common_application_pending:
            if self.common_application_pending != other.common_application_pending:
                return False
        if self.common_application_pending and other.common_application_pending:
            if self.common_application_pending != other.common_application_pending:
                return False
        if self.contract_application_pending and other.contract_application_pending:
            if self.contract_application_pending != other.contract_application_pending:
                return False
        if self.broker_application_pending and other.broker_application_pending:
            if self.broker_application_pending != other.broker_application_pending:
                return False
        if self.ins_property and other.ins_property:
            if self.ins_property != other.ins_property:
                return False
        if self.ins_passenger and other.ins_passenger:
            if self.ins_passenger != other.ins_passenger:
                return False
        if self.ins_household_goods and other.ins_household_goods:
            if self.ins_household_goods != other.ins_household_goods:
                return False
        if self.ins_private and other.ins_private:
            if self.ins_private != other.ins_private:
                return False
        if self.ins_enterprise and other.ins_enterprise:
            if self.ins_enterprise != other.ins_enterprise:
                return False
        if self.bipd_required and other.bipd_required:
            if self.bipd_required != other.bipd_required:
                return False
        if self.bipd_on_file and other.bipd_on_file:
            if self.bipd_on_file != other.bipd_on_file:
                return False
        if self.cargo_required and other.cargo_required:
            if self.cargo_required != other.cargo_required:
                return False
        if self.cargo_on_file and other.cargo_on_file:
            if self.cargo_on_file != other.cargo_on_file:
                return False
        if self.bond_required and other.bond_required:
            if self.bond_required != other.bond_required:
                return False
        if self.bond_on_file and other.bond_on_file:
            if self.bond_on_file != other.bond_on_file:
                return False

    def __neq__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "<LicenseReport(dot_number={0}), Version(version={1})>".format(self.dot_number, self.version)
