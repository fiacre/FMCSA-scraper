from sqlalchemy import Column, String, Integer
from sqlalchemy.schema import UniqueConstraint
from scraper.db.factories import DateTimeFactory
from scraper.db import JSONEncodedDict, NoSQLMeta, NoSQLProperty, SequentialUUID
from scraper.libs import sequuid
from scraper.libs.validators import ValidLength, ValidString, ValidInteger
from scraper.models import BaseModel, CUDFields
from .validators import ValidFMCSABoolean, ValidFMCSACarrierOperation, ValidFMCSADate, ValidFMCSAInteger


class CensusReport(CUDFields, BaseModel, metaclass=NoSQLMeta):

    __tablename__ = 'census'
    __table_args__ = (UniqueConstraint('dot_number', 'version', name='census_version_dot_uc'),)

    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    dot_number = Column(String(10))
    version = Column(Integer)
    body = Column(JSONEncodedDict)

    legal_name = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=128)])
    dba_name = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=128)])
    carrier_operation = NoSQLProperty(target='body', validators=[ValidFMCSACarrierOperation(), ValidLength(max_len=64)])

    hm_flag = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])
    pc_flag = NoSQLProperty(target='body', validators=[ValidFMCSABoolean()])

    phy_street = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=128)])
    phy_city = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=128)])
    phy_state = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=32)])
    phy_zip = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=16)])
    phy_country = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=128)])

    mailing_street = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=128)])
    mailing_city = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=128)])
    mailing_state = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=32)])
    mailing_zip = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=16)])
    mailing_country = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=128)])

    telephone = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=16)])
    fax = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=16)])
    email_address = NoSQLProperty(target='body', validators=[ValidString()])

    mcs150_date = NoSQLProperty(target='body', validators=[ValidFMCSADate()], factories=[DateTimeFactory()])
    mcs150_mileage = NoSQLProperty(target='body', validators=[ValidString()])
    mcs150_mileage_year = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=4)])

    add_date = NoSQLProperty(target='body', validators=[ValidFMCSADate()], factories=[DateTimeFactory()])

    oic_state = NoSQLProperty(target='body', validators=[ValidString(), ValidLength(max_len=32)])

    driver_total = NoSQLProperty(target='body', validators=[ValidString()])
    nbr_power_unit = NoSQLProperty(target='body', validators=[ValidString()])

    def __repr__(self):
        return "<CensusReport(dot_number={0}), Version(version={1}), Name(legal_name={2}>".format(
               self.dot_number, self.version, self.legal_name)
