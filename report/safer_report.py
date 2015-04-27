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
    ValidFMCSAString
)
from scraper.models import BaseModel, CUDFields
from scraper.models.fmcsa.validators import ValidFMCSADate


class SaferReport(CUDFields, BaseModel, metaclass=NoSQLMeta):
    """
        main Federal Motor Carrier Safety Administration company snapshot page
    """
    __tablename__ = 'safer'
    __table_args__ = (UniqueConstraint('dot_number', 'version', name='csa_version_dot_uc'),)

    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    dot_number = Column(String(10))
    version = Column(Integer)
    body = Column(JSONEncodedDict)

    page_date = NoSQLProperty(target='body',
        validators=[ValidFMCSAString(), ValidFMCSADate()], factories=[DateTimeFactory()]
    )
    carrier_operation = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=128)])
    status = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=16)])
    entity_type = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=32)])
    legal_name = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=128)])
    dba_name = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=128)])
    address = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=256)])
    mailing_address = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=256)])
    telephone = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=16)])
    nbr_power_unit = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    mc_number = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=32)])
    driver_total = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    #mcs150_date = NoSQLProperty(target='body', validators=[ValidFMCSADate()], factories=[DateTimeFactory()])
    mcs150_date = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=16)])
    mcs150_mileage = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    mcs150_mileage_year = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=4)])
    oos_date = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=32)])
    state_id = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=64)])
    duns_number = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=64)])

    veh_inspections = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    veh_oos = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    veh_oos_pct = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAFloat()])

    drv_inspections = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    drv_oos = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    drv_oos_pct = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAFloat()])

    hazmat_inspections = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    hazmat_oos = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    hazmat_oos_pct = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAFloat()])

    iep_inspections = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    iep_oos = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    iep_oos_pct = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAFloat()])

    fatal_crashes = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    injury_crashes = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    tow_crashes = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    total_crashes = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])

    veh_inspections_ca = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    veh_oos_ca = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    veh_oos_pct_ca = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAFloat()])

    drv_inspections_ca = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    drv_oos_ca = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    drv_oos_pct_ca = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAFloat()])

    fatal_crashes_ca = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    injury_crashes_ca = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    tow_crashes_ca = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])
    total_crashes_ca = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAInteger()])

    rating_date = NoSQLProperty(target='body',
        validators=[ValidFMCSAString(), ValidFMCSADate()], factories=[DateTimeFactory()]
    )
    rating = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=32)])
    review_date = NoSQLProperty(target='body',
        validators=[ValidFMCSAString(), ValidFMCSADate()], factories=[DateTimeFactory()]
    )
    rating_type = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=32)])

    operation_classification = NoSQLProperty(target='body', default=[])
    carrier_operation_list = NoSQLProperty(target='body', default=[])
    cargo_carried = NoSQLProperty(target='body', default=[])

    def __repr__(self):
        return "<SaferReport(dot_number={0}), Legal Name(legal_name={1}), Version(version={2})>".format(
            self.dot_number, self.legal_name, self.version)

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        #strings:
        if self.legal_name and other.legal_name:
            if self.legal_name != other.legal_name:
                return False
        if self.dba_name and other.dba_name:
            if self.dba_name != other.dba_name:
                return False
        if self.address and other.address:
            if self.address != other.address:
                return False

        if self.telephone and other.telephone:
            my_digits = re.findall(r'\d', self.telephone)
            other_digits = re.findall(r'\d', other.telephone)
            if my_digits != other_digits:
                return False
        elif self.telephone and not other.telephone:
            return False
        elif other.telephone and not self.telephone:
            return False

        # dates:
        if self.mcs150_date != other.mcs150_date:
            return False
        if self.rating_date != other.rating_date:
            return False
        if self.review_date != other.review_date:
            return False
        if self.oos_date != other.oos_date:
            return False

        # rest of fields
        if self.nbr_power_unit != other.nbr_power_unit:
            return False
        if self.mc_number != other.mc_number:
            return False
        if self.driver_total != other.driver_total:
            return False
        if self.mcs150_mileage != other.mcs150_mileage:
            return False
        if self.state_id != other.state_id:
            return False
        if self.duns_number != other.duns_number:
            return False
        if self.veh_inspections != other.veh_inspections:
            return False
        if self.veh_oos != other.veh_oos:
            return False
        if self.veh_oos_pct != other.veh_oos_pct:
            return False
        if self.drv_inspections != other.drv_inspections:
            return False
        if self.drv_oos != other.drv_oos:
            return False
        if self.drv_oos_pct != other.drv_oos_pct:
            return False
        if self.hazmat_inspections != other.hazmat_inspections:
            return False
        if self.hazmat_oos != other.hazmat_oos:
            return False
        if self.hazmat_oos_pct != other.hazmat_oos_pct:
            return False
        if self.iep_inspections != other.iep_inspections:
            return False
        if self.iep_oos != other.iep_oos:
            return False
        if self.iep_oos_pct != other.iep_oos_pct:
            return False
        if self.fatal_crashes != other.fatal_crashes:
            return False
        if self.injury_crashes != other.injury_crashes:
            return False
        if self.tow_crashes != other.tow_crashes:
            return False
        if self.total_crashes != other.total_crashes:
            return False
        if self.veh_inspections_ca != other.veh_inspections_ca:
            return False
        if self.veh_oos_ca != other.veh_oos_ca:
            return False
        if self.veh_oos_pct_ca != other.veh_oos_pct_ca:
            return False
        if self.drv_inspections_ca != other.drv_inspections_ca:
            return False
        if self.drv_oos_ca != other.drv_oos_ca:
            return False
        if self.drv_oos_pct_ca != other.drv_oos_pct_ca:
            return False
        if self.fatal_crashes_ca != other.fatal_crashes_ca:
            return False
        if self.injury_crashes_ca != other.injury_crashes_ca:
            return False
        if self.tow_crashes_ca != other.tow_crashes_ca:
            return False
        if self.total_crashes_ca != other.total_crashes_ca:
            return False
        if self.rating != other.rating:
            return False
        if self.rating_type != other.rating_type:
            return False
        if self.operation_classification != other.operation_classification:
            return False
        if self.carrier_operation_list != other.carrier_operation_list:
            return False
        if self.cargo_carried != other.cargo_carried:
            return False

        return True

    def __neq__(self, other):
        return not self.__eq__(other)
