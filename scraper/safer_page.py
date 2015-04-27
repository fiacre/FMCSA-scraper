from io import StringIO
from lxml import etree
import re
from scraper.models.fmcsa.scraper_property import (
    ScraperProperty,
    ScraperDateProperty,
    ScraperListProperty
)
from scraper.models.fmcsa.exceptions import(
    BadDOTNumber,
    RecordNotFound
)
from .base_page import BasePage
from scraper.models.fmcsa import FMCSAManager
from scraper.models.fmcsa.report import SaferReport
import transaction


class SaferPage(BasePage):
    """ representation of safer/fmcsa.dot.gov
        public class members are descriptors

        address : address of carrier
        cargo_carried : list, type of cargo transported by the carrier/shipper
        carrier_operation: authorized, active or other
        dba_name: doing business as
        driver_total:
            Total number of drivers employed by the carrier/shipper.
        drv|iep|veh|hazmat inspection: num inspections per driver, iep, vehicle, hazmat
            also vehilc and driver inspections for Canada
        drv|iep|veh|haz oos: out of Service total, out of service percetage
            also vehilc and driver out of service total and pct for Canada
        fatal|injury|tow|total crashes in last 24 months inluding Canada
        operation_classification: list of motor carrier type
        state id:Company I.D. number assigned by the state,
        duns number:  corporate registration number given by Dun & Bradstreet
        mc_number -- deprecated but still searchable
            (UPS has multi MC nums -- only known exception)
        nbr_power_unit:
            number of Trucks, Tractors, Hazardous Material Tank Trucks,
            Motor Coaches, School Buses, Mini-Bus/Vans and Limousines
            owned, term leased or trip leased by the motor carrier.
        MCS-150 Form Date:
            Date from the MCS-150 Registration Form.
        MCS-150 Form Mileage:
            Mileage from the MCS-150 Registration Form.
        Carrier Safety Rating:
            measure of the carrier's compliance with the Federal Motor Carrier Safety Regulations
            one of:
                Satisfactory
                Conditional - carrier was out of compliance with one or more safety requirements.
                Unsatisfactory - evidence of substantial noncompliance with safety requirements.
    """
    address = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[6]/td/text()'
    )
    cargo_carried = ScraperListProperty(
        expr='//table[@summary="Cargo Carried"]'
    )
    carrier_operation = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[3]/td[1]'
    )
    carrier_operation_list = ScraperListProperty(
        expr='//table[@summary="Carrier Operation"]'
    )
    dba_name = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[5]/td[1]'
    )
    driver_total = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[11]/td[2]'
    )
    drv_inspections = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[2]/td[2]'
    )
    drv_inspections_ca = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[6]/table/tr[2]/td[2]'
    )
    drv_oos = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[3]/td[2]'
    )
    drv_oos_ca = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[6]/table/tr[3]/td[2]'
    )
    drv_oos_pct = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[4]/td[2]'
    )
    drv_oos_pct_ca = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[6]/table/tr[4]/td[2]'
    )
    duns_number = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[10]/td[2]'
    )
    entity_type = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[2]/td[1]'
    )
    fatal_crashes = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[4]/table/tr[2]/td[1]'
    )
    fatal_crashes_ca = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[7]/table/tr[2]/td[1]'
    )
    hazmat_inspections = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[2]/td[3]'
    )
    hazmat_oos = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[3]/td[3]'
    )
    hazmat_oos_pct = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[4]/td[3]'
    )
    iep_inspections = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[2]/td[4]'
    )
    iep_oos = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[3]/td[4]'
    )
    iep_oos_pct = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[4]/td[4]'
    )
    injury_crashes = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[4]/table/tr[2]/td[2]'
    )
    injury_crashes_ca = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[7]/table/tr[2]/td[2]'
    )
    legal_name = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[4]/td[1]'
    )
    mailing_address = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[8]/td/text()'
    )
    mc_number = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[10]/td[1]'
    )
    mcs150_date = ScraperDateProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[12]/td[1]'
    )
    mcs150_mileage_and_year = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[12]/td[2]'
    )
    nbr_power_unit = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[11]/td[1]'
    )
    oos_date = ScraperDateProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[3]/td[2]'
    )
    operation_classification = ScraperListProperty(
        expr='//table[@summary="Operation Classification"]'
    )
    page_date = ScraperDateProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/table/tr[3]/td/font/b[3]/font'
    )
    rating = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[9]/table/tr[3]/td[1]'
    )
    rating_date = ScraperDateProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[9]/table/tr[2]/td[1]'
    )
    rating_type = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[9]/table/tr[3]/td[2]'
    )
    review_date = ScraperDateProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[9]/table/tr[2]/td[2]'
    )
    state_id = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[9]/td[2]'
    )
    telephone = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[1]/table/tr[7]/td'
    )
    total_crashes = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[4]/table/tr[2]/td[4]'
    )
    total_crashes_ca = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[7]/table/tr[2]/td[4]'
    )
    tow_crashes = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[4]/table/tr[2]/td[3]'
    )
    tow_crashes_ca = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[7]/table/tr[2]/td[3]'
    )
    veh_inspections = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[2]/td[1]'
    )
    veh_inspections_ca = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[6]/table/tr[2]/td[1]'
    )
    veh_oos = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[3]/td[1]'
    )
    veh_oos_ca = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[6]/table/tr[3]/td[1]'
    )
    veh_oos_pct = ScraperProperty(
        expr='//table/tr[2]/td/table/tr[2]/td/center[3]/table/tr[4]/td[1]'
    )
    veh_oos_pct_ca = ScraperProperty(
        expr='//table//tr[2]/td/table/tr[2]/td/center[6]/table/tr[4]/td[1]'
    )

    def __init__(self, dot_number, timeout=BasePage.DEFAULT_TIMEOUT):
        """
            @dot_number : use DOT number for search
            @timeout: override default timeout of 120 seconds
            set dot, timeout, data manager and HTML for instance
        """
        super().__init__()
        self.dot_number = dot_number
        url = self.url_data['safer']  # http://safer.fmcsa.dot.gov/query.asp'
        self.html_doc = super()._get_html(url, timeout)
        self.root = etree.parse(StringIO(self.html_doc), self.parser)
        self.manager = FMCSAManager()

    def _is_valid(self, dot_number):
        """
            @dot_number: DOT to be searched
            if page indicates no record, we raise BadDOTNumber
            else return true
        """
        valid_record = self.root.xpath('//table/tr/td/font/text()')

        if valid_record and len(valid_record):
            if re.match(r'^No records matching', valid_record[0].strip()):
                raise RecordNotFound("{0} is not a valid DOT number".format(dot_number))
            elif re.match(r'^The record matching', valid_record[0].strip()):
                raise BadDOTNumber(
                    '{0} is not a currently active DOT number'.format(dot_number)
                )
            else:
                return True

    def _split_mileage_and_year(self):
        """
            helper method for mcs 150 data
            one td has miles (year) -- strip
            parens and keep
            them as separate fields in the ORM
            same as census report
        """
        if self.mcs150_mileage_and_year:
            try:
                (mcs150_mileage, mcs150_mileage_year) = self.mcs150_mileage_and_year.split()
                self.mcs150_mileage_year = mcs150_mileage_year.replace('(', '')
                self.mcs150_mileage_year = self.mcs150_mileage_year.replace(')', '')
                self.mcs150_mileage = mcs150_mileage
            except ValueError:
                self.mcs150_mileage_year = None
                self.mcs150_mileage = None
        else:
            self.mcs150_mileage = None
            self.mcs150_mileage_year = None

    def create_safer_report(self):
        """
            public method to create SaferReport
        """
        if not self._is_valid(self.dot_number):
            raise BadDOTNumber('{0} is not in the SAFER system'.format(self.dot_number))
        report = SaferReport()
        report.dot_number = self.dot_number
        report.address = self.address
        report.cargo_carried = self.cargo_carried
        report.carrier_operation = self.carrier_operation
        report.carrier_operation_list = self.carrier_operation_list
        report.driver_total = self.driver_total
        report.drv_inspections_ca = self.drv_inspections_ca
        report.drv_oos_ca = self.drv_oos_ca
        report.drv_oos_pct_ca = self.drv_oos_pct_ca
        report.duns_number = self.duns_number
        report.entity_type = self.entity_type
        report.fatal_crashes = self.fatal_crashes
        report.fatal_crashes_ca = self.fatal_crashes_ca
        report.hazmat_inspections = self.hazmat_inspections
        report.hazmat_oos = self.hazmat_oos
        report.hazmat_oos_pct = self.hazmat_oos_pct
        report.iep_inspections = self.iep_inspections
        report.iep_oos = self.iep_oos
        report.iep_oos_pct = self.iep_oos_pct
        report.injury_crashes = self.injury_crashes
        report.injury_crashes_ca = self.injury_crashes_ca
        report.legal_name = self.legal_name
        report.mailing_address = self.mailing_address
        report.mc_number = self.mc_number
        report.mcs150_date = self.mcs150_date
        self._split_mileage_and_year()
        report.mcs150_mileage = self.mcs150_mileage
        report.mcs150_mileage_year = self.mcs150_mileage_year
        report.nbr_power_unit = self.nbr_power_unit
        report.oos_date = self.oos_date
        report.operation_classification = self.operation_classification
        report.page_date = self.page_date
        report.rating = self.rating
        report.rating_date = self.rating_date
        report.rating_type = self.rating_type
        report.review_date = self.review_date
        report.state_id = self.state_id
        report.telephone = self.telephone
        report.total_crashes = self.total_crashes
        report.total_crashes_ca = self.total_crashes_ca
        report.tow_crashes = self.tow_crashes
        report.tow_crashes_ca = self.tow_crashes_ca
        report.veh_inspections = self.veh_inspections
        report.veh_inspections_ca = self.veh_inspections_ca
        report.veh_oos = self.veh_oos
        report.veh_oos_ca = self.veh_oos_ca
        report.veh_oos_pct = self.veh_oos_pct
        report.veh_oos_pct_ca = self.veh_oos_pct_ca

        try:
            self.manager.add_scraped_report(self.dot_number, report)
            transaction.commit()
        except Exception as e:
            print("Caught Exception on transaction commit: {0}".format(str(e)))
