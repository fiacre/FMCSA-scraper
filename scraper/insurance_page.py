from io import StringIO
from lxml import etree
import re
from scraper.models.fmcsa.scraper_property import (
    ScraperRowProperty,
    ScraperTableProperty,
    ScraperRevocationProperty
)
from .base_page import BasePage
from scraper.models.fmcsa.exceptions import NoScrapedRows, RecordNotFound
from scraper.models.fmcsa.report import (
    ActiveInsuranceReport,
    AuthorityHistoryReport,
    InsuranceHistoryReport,
    PendingApplicationReport,
    RejectedInsuranceReport,
    RevocationReport
)
from scraper.models.fmcsa import FMCSAManager
import transaction
from sqlalchemy.exc import IntegrityError
from scraper.config import Settings


class InsuranceBasePage(BasePage):
    def __init__(self, dot_number, timeout=BasePage.DEFAULT_TIMEOUT):
        super().__init__()
        self.dot_number = dot_number
        self._set_url()
        # if get html fails urllib or socket.timeout error is raised
        self.html_doc = super()._get_html(self.url, timeout)
        self.root = etree.parse(StringIO(self.html_doc), self.parser)
        self.manager = FMCSAManager()

    def _has_data(self):
        """
            sub pages with no data
            are indicated with strong red font as
            "No Data Available"
        """
        no_records = self.root.xpath('//strong/font[@color="red"]')
        if no_records and len(no_records):
            result = str(no_records[0].text)
            if re.match(r'^No Data Available$', result.strip()):
                return False
            else:
                return True
        return True

    def _set_url(self):
        url_data = Settings().fmcsa_urls
        self.url = url_data['insurance_base']

    def create_report(self):
        pass


class InsuranceHistoryPage(InsuranceBasePage):
    """
        define descriptor
        @expr:xpath expression for html table
        @fields:field names used in orm
    """
    insurance_policies = ScraperRowProperty(
        expr='//font/table[4]',
        fields=['form_name',
                'insurance_type',
                'carrier',
                'policy_name',
                'coverage_from',
                'coverage_to',
                'date_from',
                'status',
                'date_to'
                ]
    )

    def _set_url(self):
        url_data = Settings().fmcsa_urls
        self.url = url_data['insurance_history']

    def create_report(self):
        """
            @insurance_policies: list of dictionaries
                populated by descriptor
        """
        if not self._have_dot_number(self.dot_number):
            raise RecordNotFound('no safer record stored for {0}'.format(self.dot_number))
        if not self._has_data():
            raise NoScrapedRows('{0} has no insurance history'.format(self.dot_number))
        for row in self.insurance_policies:
            report = InsuranceHistoryReport()
            report.dot_number = self.dot_number
            report.form_name = row.get('form_name')
            report.insurance_type = row.get('insurance_type')
            report.carrier = row.get('carrier')
            report.policy_name = row.get('policy_name')
            report.coverage_from = row.get('coverage_from')
            report.coverage_to = row.get('coverage_to')
            report.date_from = row.get('date_from')
            report.date_to = row.get('date_to')
            report.status = row.get('status')
            try:
                self.manager.add_insurance_report(self.dot_number, report, cls=InsuranceHistoryReport)
            except Exception as e:
                print("Caught Exception on transaction commit: {0}".format(str(e)))
                continue
            else:
                try:
                    transaction.commit()
                except IntegrityError as e:
                    print("IntegrityError: {0}, continuing".format(str(e)))
                    transaction.abort()
                    continue


class ActiveInsurancePage(InsuranceBasePage):
    insurance_policies = ScraperRowProperty(
        expr='//font/table[4]',
        fields=['form_name',
                'insurance_type',
                'carrier',
                'policy_name',
                'posted_date',
                'coverage_from',
                'coverage_to',
                'effective_date',
                'cancellation_date'
                ]
    )

    def _set_url(self):
        url_data = Settings().fmcsa_urls
        self.url = url_data['insurance_active']

    def create_report(self):
        if not self._has_data():
            raise NoScrapedRows("{0} has no active insurance data".format(self.dot_number))
        for row in self.insurance_policies:
            report = ActiveInsuranceReport()
            report.dot_number = self.dot_number
            report.form_name = row.get('form_name')
            report.insurance_type = row.get('insurance_type')
            report.carrier = row.get('carrier')
            report.policy_name = row.get('policy_name')
            report.posted_date = row.get('posted_date')
            report.coverage_from = row.get('coverage_from')
            report.coverage_to = row.get('coverage_to')
            report.effective_date = row.get('effective_date')
            report.cancellation_date = row.get('cancellation_date')
            try:
                self.manager.add_insurance_report(self.dot_number, report, cls=ActiveInsuranceReport)
            except Exception as e:
                print("Caught Exception on transaction commit: {0}".format(str(e)))
                continue
            else:
                try:
                    transaction.commit()
                except IntegrityError as e:
                    print("IntegrityError: {0}, continuing".format(str(e)))
                    transaction.abort()
                    continue


class RejectedInsurancePage(InsuranceBasePage):
    insurance_policies = ScraperTableProperty(
        expr='//font/table[4]',
        fields=['form_name',
                'insurance_type',
                'carrier',
                'policy_name',
                'coverage_from',
                'coverage_to',
                'received_date',
                'rejected_date'
                ]
    )

    def _set_url(self):
        url_data = Settings().fmcsa_urls
        self.url = url_data['insurance_rejected']

    def create_report(self):
        if not self._has_data():
            raise NoScrapedRows("{0} has no insurance history data".format(self.dot_number))
        for row in self.insurance_policies:
            report = RejectedInsuranceReport()
            report.dot_number = self.dot_number
            report.form_name = row.get('form_name')
            report.insurance_type = row.get('insurance_type')
            report.carrier = row.get('carrier')
            report.policy_name = row.get('policy_name')
            report.coverage_from = row.get('coverage_from')
            report.coverage_to = row.get('coverage_to')
            report.received_date = row.get('received_date')
            report.rejected_date = row.get('rejected_date')
            try:
                self.manager.add_insurance_report(self.dot_number, report, cls=RejectedInsuranceReport)
            except Exception as e:
                print("Caught Exception on transaction commit: {0}".format(str(e)))
                continue
            else:
                try:
                    transaction.commit()
                except IntegrityError as e:
                    print("IntegrityError: {0}, continuing".format(str(e)))
                    transaction.abort()
                    continue


class AuthorityHistoryPage(InsuranceBasePage):
    policies = ScraperTableProperty(
        expr='//font/table[4]',
        fields=['auth_type',
                'action',
                'action_date',
                'dispostion',
                'dispostion_date'
                ]
    )

    def _set_url(self):
        url_data = Settings().fmcsa_urls
        self.url = url_data['authority_history']

    def create_report(self):
        if not self._has_data():
            raise NoScrapedRows("{0} has no insurance history data".format(self.dot_number))
        for row in self.policies:
            report = AuthorityHistoryReport()
            report.dot_number = self.dot_number
            report.auth_type = row.get('auth_type')
            report.action = row.get('action')
            report.action_date = row.get('action_date')
            report.dispostion = row.get('dispostion')
            report.dispostion_date = row.get('dispostion_date')
            try:
                self.manager.add_insurance_report(self.dot_number, report, cls=AuthorityHistoryReport)
            except Exception as e:
                print("Caught Exception on transaction commit: {0}".format(str(e)))
                continue
            else:
                try:
                    transaction.commit()
                except IntegrityError as e:
                    print("IntegrityError: {0}, continuing".format(str(e)))
                    transaction.abort()
                    continue


class PendingApplicationPage(InsuranceBasePage):
    policies = ScraperRowProperty(
        expr='//font/table[4]',
        fields=['auth_type',
                'file_date',
                'insurance',
                'boc_3'
                ]
    )

    def _set_url(self):
        url_data = Settings().fmcsa_urls
        self.url = url_data['insurance_pending']

    def create_report(self):
        if not self._has_data():
            raise NoScrapedRows("{0} has no insurance history data".format(self.dot_number))
        for row in self.policies:
            report = PendingApplicationReport()
            report.dot_number = self.dot_number
            report.auth_type = row.get('auth_type')
            report.file_date = row.get('file_date')
            report.insurance = row.get('insurance')
            report.boc_3 = row.get('boc_3')
            try:
                self.manager.add_insurance_report(self.dot_number, report, cls=PendingApplicationPage)
            except Exception as e:
                print("Caught Exception on transaction commit: {0}".format(str(e)))
                continue
            else:
                try:
                    transaction.commit()
                except IntegrityError as e:
                    print("IntegrityError: {0}, continuing".format(str(e)))
                    transaction.abort()
                    continue


class RevocationPage(InsuranceBasePage):
    policies = ScraperRevocationProperty(
        expr='//font/table[4]',
        fields=['auth_type',
                'initial_date',
                'effective_date',
                'reason'
                ]
    )

    def _set_url(self):
        url_data = Settings().fmcsa_urls
        self.url = url_data['insurance_revocation']

    def create_report(self):
        if not self._has_data():
            raise NoScrapedRows("{0} has no insurance history data".format(self.dot_number))
        for row in self.policies:
            report = RevocationReport()
            report.dot_number = self.dot_number
            report.auth_type = row.get('auth_type')
            report.initial_date = row.get('initial_date')
            report.effective_date = row.get('effective_date')
            report.reason = row.get('reason')
            try:
                self.manager.add_insurance_report(self.dot_number, report, cls=RevocationReport)
            except Exception as e:
                print("Caught Exception on transaction commit: {0}".format(str(e)))
                continue
            else:
                try:
                    transaction.commit()
                except IntegrityError as e:
                    print("IntegrityError: {0}, continuing".format(str(e)))
                    transaction.abort()
                    continue
