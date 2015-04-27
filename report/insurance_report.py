import re
import dateutil
from sqlalchemy import Column, String, Integer
from sqlalchemy.schema import UniqueConstraint, ForeignKeyConstraint
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
from scraper.models.fmcsa.validators import ValidFMCSADate, ValidFMCSAMoney


class ActiveInsuranceReport(CUDFields, BaseModel, metaclass=NoSQLMeta):
    """ ORM definition for ActiveInsuranceReport """

    __tablename__ = 'active_insurance'
    __table_args__ = (UniqueConstraint(
        'dot_number',
        'form_name',
        'insurance_type',
        'policy_name',
        'effective_date',
        name='ihs_uc'
    ),
    )

    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    dot_number = Column(String(10), nullable=False)
    policy_name = Column(String(128))
    effective_date = Column(String(32))
    insurance_type = Column(String(32))
    form_name = Column(String(32))

    body = Column(JSONEncodedDict)
    carrier = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=128)])
    coverage_from = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidFMCSAMoney()])
    coverage_to = NoSQLProperty(target='body')
    posted_date = NoSQLProperty(
        target='body',
        validators=[ValidFMCSAString(), ValidFMCSADate()],
        factories=[DateTimeFactory()]
    )
    cancellation_date = NoSQLProperty(target='body')
    ForeignKeyConstraint(['dot_number'], ['safer.dot_number'])

    def __repr__(self):
        """ used for debugging """
        return "<ActiveInsuranceReport(dot_number={0}), Type(type={1})>".format(self.dot_number, self.insurance_type)


class RejectedInsuranceReport(BaseModel):
    """ ORM Definition for RejectedInsuranceReport """
    __tablename__ = 'rejected_insurance'
    __table_args__ = (UniqueConstraint('dot_number', 'policy_name', 'rejected_date', name='rir_uc'),)

    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    dot_number = Column(String(10), nullable=False)
    policy_name = Column(String(228))
    rejected_date = Column(String(16))
    insurance_type = Column(String(128))
    carrier = Column(String(128))
    coverage_from = Column(String(128))
    coverage_to = Column(String(128))
    received_date = Column(String(32))

    ForeignKeyConstraint(['dot_number'], ['safer.dot_number'])

    def __repr__(self):
        """ used for debugging """
        return "<RejectedInsuranceReport(dot_number={0}), Type(type={1})>".format(self.dot_number, self.insurance_type)


class InsuranceHistoryReport(CUDFields, BaseModel, metaclass=NoSQLMeta):
    """ ORM definition for InsuranceHistoryReport """
    __tablename__ = 'insurance_history'
    __table_args__ = (UniqueConstraint('dot_number', 'policy_name', 'date_to', name='ais_uc'),)

    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    dot_number = Column(String(10), nullable=False)
    policy_name = Column(String(128))
    date_to = Column(String(16))
    body = Column(JSONEncodedDict)

    form_name = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=8)])
    insurance_type = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=128)])
    carrier = NoSQLProperty(target='body', validators=[ValidFMCSAString(), ValidLength(max_len=128)])
    coverage_from = NoSQLProperty(target='body')
    coverage_to = NoSQLProperty(target='body')
    date_from = NoSQLProperty(target='body')
    status = NoSQLProperty(target='body')
    ForeignKeyConstraint(['dot_number'], ['safer.dot_number'])

    def __repr__(self):
        """ use for debugging """
        return "<InsuranceHistoryReport(dot_number={0}), Type(type={1})>".format(self.dot_number, self.insurance_type)


class AuthorityHistoryReport(BaseModel):
    """ ORM Definition from AuthorityHistoryReport """
    __tablename__ = 'authority_history'
    __table_args__ = (UniqueConstraint('dot_number', 'action', 'action_date'),)

    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    dot_number = Column(String(10), nullable=False)
    auth_type = Column(String(128))
    action = Column(String(128))
    action_date = Column(String(128))
    dispostion = Column(String(128))
    dispostion_date = Column(String(32))

    ForeignKeyConstraint(['dot_number'], ['safer.dot_number'])

    def __repr__(self):
        """ used for debugging """
        return "<AuthorityHistoryReport(dot_number={0}, auth_type={1}, action={2})>".format(
            self.dot_number, self.auth_type, self.action)


class PendingApplicationReport(BaseModel):
    """ ORM Definition for PendingApplicationReport """
    __tablename__ = 'pending_application'
    __table_args__ = (UniqueConstraint('dot_number', 'insurance', 'file_date', name='par_uc'),)

    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    dot_number = Column(String(10), nullable=False)
    auth_type = Column(String(128))
    file_date = Column(String(16))
    insurance = Column(String(128))
    boc_3 = Column(String(128))
    ForeignKeyConstraint(['dot_number'], ['safer.dot_number'])

    def __repr__(self):
        """ used for debugging """
        return "<PendingApplicationReport(dot_number={0}), (insurance={1})>".format(self.dot_number, self.insurance)


class RevocationReport(BaseModel):
    """ ORM Definition of RevocationReport """
    __tablename__ = 'revocation_history'
    __table_args__ = (UniqueConstraint('dot_number', 'auth_type', 'effective_date'),)

    uuid = Column(SequentialUUID, primary_key=True, default=sequuid)
    dot_number = Column(String(10), nullable=False)
    auth_type = Column(String(128))
    effective_date = Column(String(32))
    reason = Column(String(128))
    initial_date = Column(String(32))
    ForeignKeyConstraint(['dot_number'], ['safer.dot_number'])

    def __repr__(self):
        """ used for debugging """
        return "<RevocationReport(dot_number={0}, auth_type={1})>".format(self.dot_number, self.auth_type)
