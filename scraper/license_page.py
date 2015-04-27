from io import StringIO
from lxml import etree
from scraper.models.fmcsa.scraper_property import ScraperProperty
from .base_page import BasePage
from scraper.models.fmcsa.exceptions import NoScrapedRows, TimeoutError, RecordNotFound
from scraper.models.fmcsa import FMCSAManager
from scraper.models.fmcsa.report import LicenseReport
import transaction


class LicensePage(BasePage):
    common_authority_status = ScraperProperty(expr='//font/table[6]/tr[2]/td[1]/center/font')
    contract_authority_status = ScraperProperty(expr='//font/table[6]/tr[3]/td[1]/center/font')
    broker_authority_status = ScraperProperty(expr='//font/table[6]/tr[4]/td[1]/center/font')
    common_application_pending = ScraperProperty(expr='//font/table[6]/tr[2]/td[2]/center/font')
    contract_application_pending = ScraperProperty(expr='//font/table[6]/tr[3]/td[2]/center/font')
    ins_property = ScraperProperty(expr='//font/table[7]/tr[2]/td[1]/center/font')
    ins_passenger = ScraperProperty(expr='//font/table[7]/tr[2]/td[2]/center/font')
    ins_household_goods = ScraperProperty(expr='//font/table[7]/tr[2]/td[3]/center/font')
    ins_private = ScraperProperty(expr='//font/table[7]/tr[2]/td[4]/center/font')
    ins_enterprise = ScraperProperty(expr='//font/table[7]/tr[2]/td[5]/center/font')
    bipd_required = ScraperProperty(expr='//font/table[8]/tr[2]/td[1]/center/font')
    bipd_on_file = ScraperProperty(expr='//font/table[8]/tr[2]/td[2]/center/font')
    cargo_required = ScraperProperty(expr='//font/table[8]/tr[3]/td[1]/center/font')
    cargo_on_file = ScraperProperty(expr='//font/table[8]/tr[3]/td[2]/center/font')
    bond_required = ScraperProperty(expr='//font/table[8]/tr[4]/td[1]/center/font')
    bond_on_file = ScraperProperty(expr='//font/table[8]/tr[4]/td[2]/center/font')

    def __init__(self, dot_number, timeout=BasePage.DEFAULT_TIMEOUT):
        super().__init__()
        self.dot_number = dot_number
        url = 'http://li-public.fmcsa.dot.gov/LIVIEW/pkg_carrquery.prc_getdetail'
        self.html_doc = super()._get_html(url, timeout)
        if not self.html_doc:
            raise TimeoutError("Timeout exceeded getting page for {0}".format(dot_number))
        self.root = etree.parse(StringIO(self.html_doc), self.parser)
        self.manager = FMCSAManager()

    def _has_data(self, dot_number):
        # if the html returned is the license detail page
        # then the DOT number is in upper right corner
        record = self.root.xpath('//font/table[3]/tr[1]/td[1]/font')
        if len(record) and record[0].text.strip() == self.dot_number:
            return True
        else:
            return False

    def create_report(self):
        if not self._has_data(self.dot_number):
            raise NoScrapedRows('{0} has no license data'.format(self.dot_number))
        if not self._have_dot_number(self.dot_number):
            raise RecordNotFound('no safer record stored for {0}'.format(self.dot_number))
        report = LicenseReport()
        report.dot_number = self.dot_number
        report.common_authority_status = self.common_authority_status
        report.broker_authority_status = self.broker_authority_status
        report.common_application_pending = self.common_application_pending
        report.contract_application_pending = self.contract_application_pending
        report.ins_property = self.ins_property
        report.ins_passenger = self.ins_passenger
        report.ins_household_goods = self.ins_household_goods
        report.ins_private = self.ins_private
        report.ins_enterprise = self.ins_enterprise
        report.bipd_required = self.bipd_required
        report.bipd_on_file = self.bipd_on_file
        report.cargo_required = self.cargo_required
        report.cargo_on_file = self.cargo_on_file
        report.bond_required = self.bond_required
        report.bond_on_file = self.bond_on_file

        try:
            self.manager.add_scraped_report(self.dot_number, report, cls=LicenseReport)
            transaction.commit()
        except Exception as e:
            print("Caught Exception on transaction commit: {0}".format(str(e)))
