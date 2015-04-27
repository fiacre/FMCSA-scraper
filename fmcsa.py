from sqlalchemy import and_
from sqlalchemy.sql import func
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError
from scraper.db import Shard
from .census import CensusReport
from .sms import SMSReport
from .report import (
    ActiveInsuranceReport,
    AuthorityHistoryReport,
    InsuranceHistoryReport,
    LicenseReport,
    PendingApplicationReport,
    RejectedInsuranceReport,
    RevocationReport,
    SaferReport
)


class FMCSAManager:
    __shardname__ = 'fmcsa'

    @classmethod
    def __setup__(cls):
        shard = Shard.create(cls.__shardname__, join_transaction=False)
        CensusReport._create_table(shard)
        SaferReport._create_table(shard)
        SMSReport._create_table(shard)
        LicenseReport._create_table(shard)
        InsuranceHistoryReport._create_table(shard)
        ActiveInsuranceReport._create_table(shard)
        RejectedInsuranceReport._create_table(shard)
        AuthorityHistoryReport._create_table(shard)
        PendingApplicationReport._create_table(shard)
        RevocationReport._create_table(shard)

    def __init__(self):
        self.shard = Shard(self.__shardname__)

    def add_versioned_report(self, report_type, report):
        latest_version = self.get_latest_version_number(report_type, report.dot_number)

        if latest_version:
            report.version = latest_version + 1
        else:
            report.version = 1

        self.shard.add(report)

    def get_latest_version_number(self, cls, dot_number):
        return self.shard.query(func.max(cls.version)).filter(cls.dot_number == dot_number).scalar()

    def get_latest_report(self, report_type, dot_number):
        latest_version = self.get_latest_version_number(report_type, dot_number)

        where = and_(report_type.dot_number == dot_number,
                     report_type.version == latest_version)

        return self.shard.query(report_type).filter(where).one()

    def add_scraped_report(self, dot_number, report, cls=SaferReport):
        try:
            latest_report = self.get_latest_report(cls, dot_number)
        except NoResultFound:
            # there is no previous report
            # for this DOT number, create one
            self.add_versioned_report(cls, report)
        else:
            #  There is a previous report
            if report != latest_report:
                #  But it is different than this new one
                #  save a new version
                self.add_versioned_report(cls, report)

    def add_insurance_report(self, dot_number, report, cls=InsuranceHistoryReport):
        try:
            self.shard.add(report)
        except IntegrityError as e:
            # log this
            pass
