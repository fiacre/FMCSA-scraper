from sqlalchemy import Column, String, Integer, Boolean
from sqlalchemy.schema import UniqueConstraint
from scraper.db import JSONEncodedDict, NoSQLMeta, NoSQLProperty, SequentialUUID
from scraper.libs import sequuid
from scraper.libs.validators import ValidInteger, ValidMinMax
from scraper.models import BaseModel, CUDFields
from .validators import ValidFMCSABoolean, ValidFMCSAFloat


class SMSReport(CUDFields, BaseModel, metaclass=NoSQLMeta):
    """
        ORM definition for Safety Management System monthly report
    """

    __tablename__ = 'sms'
    __table_args__ = (UniqueConstraint('dot_number', 'version', name='sms_verion_dot_uc'),)

    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    dot_number = Column(String(10))
    version = Column(Integer)
    hazmat = Column(Boolean)
    body = Column(JSONEncodedDict)

    inspection_total = NoSQLProperty(target='body', validators=[ValidInteger()])
    driver_inspection_total = NoSQLProperty(target='body', validators=[ValidInteger()])
    driver_oos_inspection_total = NoSQLProperty(target='body', validators=[ValidInteger()])
    vehicle_inspection_total = NoSQLProperty(target='body', validators=[ValidInteger()])
    vehicle_oos_inspection_total = NoSQLProperty(target='body', validators=[ValidInteger()])

    unsafe_driver_percent = NoSQLProperty(target='body',
                                          validators=[ValidFMCSAFloat(), ValidMinMax(min_val=0, max_val=100)])
    unsafe_driver_over_threshold = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    unsafe_driver_serious_violation = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    unsafe_driver_alert = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])

    fatigue_driver_percent = NoSQLProperty(target='body',
                                           validators=[ValidFMCSAFloat(), ValidMinMax(min_val=0, max_val=100)])
    fatigue_driver_over_threshold = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    fatigue_driver_serious_violation = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    fatigue_driver_alert = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])

    driver_fitness_percent = NoSQLProperty(target='body',
                                           validators=[ValidFMCSAFloat(), ValidMinMax(min_val=0, max_val=100)])
    driver_fitness_over_threshold = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    driver_fitness_serious_violation = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    driver_fitness_alert = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])

    controlled_substances_percent = NoSQLProperty(target='body',
                                                  validators=[ValidFMCSAFloat(), ValidMinMax(min_val=0, max_val=100)])
    controlled_substances_over_threshold = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    controlled_substances_serious_violation = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    controlled_substances_alert = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])

    vehicle_maintenance_percent = NoSQLProperty(target='body',
                                                validators=[ValidFMCSAFloat(), ValidMinMax(min_val=0, max_val=100)])
    vehicle_maintenance_over_threshold = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    vehicle_maintenance_serious_violation = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    vehicle_maintenance_alert = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])

    def __repr__(self):
        return "<SMSReport(dot_number='{0},Version={1})>".format(self.dot_number, self.version)
