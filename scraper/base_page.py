from io import StringIO
from lxml import etree
from urllib import parse, request
from scraper.models.fmcsa.exceptions import TimeoutError
from urllib.error import HTTPError, URLError
from socket import timeout as sock_timeout
from scraper.models.fmcsa.report import SaferReport
from scraper.config import Settings
#from random import choice


class BasePage:
    """
        Base Page for Safer and Insurance/License Pages
        define timeout
        set html parser
    """
    DEFAULT_TIMEOUT = 120

    def __init__(self, *args, **kwargs):
        for name, value in kwargs.items():
            setattr(self, name, value)

        self.parser = etree.HTMLParser()
        settings = Settings()
        self.url_data = settings.fmcsa_urls

    def _query_params(self, url):
        """
            @url: either the safer url or an insurance url
            returns url_encoded parameters for the safer url_data
                OR
            return the params for an insurance url
            insurance url's use their internal key, not dot_number
            if @url is an insurance url, calls self._get_html to get internal id
        """
        if url == self.url_data['safer']:
            params = {
                "searchtype": "ANY",
                "query_type": "queryCarrierSnapshot",
                "query_param": "USDOT",
                "query_string": self.dot_number
            }
            return params
        elif url == self.url_data['insurance_base']:
            params = {
                'n_dotno': self.dot_number
            }
            return params
        elif url in (self.url_data['insurance_detail'],
                     self.url_data['insurance_active'],
                     self.url_data['insurance_rejected'],
                     self.url_data['insurance_history'],
                     self.url_data['authority_history'],
                     self.url_data['insurance_pending'],
                     self.url_data['insurance_revocation']):
            params = {}
            html = self._get_html(self.url_data['insurance_base'])
            root = etree.parse(StringIO(html), self.parser)
            # Find the HTML Button (form)
            # get the internal id (pv_apcant_id) of
            # the insurance record for this dot_number
            for form in root.xpath('//form[@action="pkg_carrquery.prc_getdetail"]'):
                for input_type in form.getchildren():
                    name = input_type.attrib.get('name')
                    value = input_type.attrib.get('value')
                    if name in ('pv_apcant_id', 'pv_vpath'):
                        params[name] = value
            return params

    def _get_html(self, url, timeout=DEFAULT_TIMEOUT):
        """
            @url
            @timeout
            create a request object based on @url
            fetch and return HTML based on that request object
            calls self._query_params to create request
            reraise errors: want to log these here and handle them
            in calling code -- might be a better approach???
        """
        data = parse.urlencode(self._query_params(url)).encode('utf-8')
        try:
            req = request.Request(url, data)
            response = request.urlopen(req, timeout=timeout)
            html = response.read().decode('ISO-8859-1')
        except (HTTPError, URLError) as error:
            print("Caught Error fetching from {0} {1}: {2}".format(url, str(data), str(error)))
            raise(error)
        except sock_timeout:
            raise(TimeoutError("TimeoutError: {0}, {1}".format(url, str(data))))
        else:
            return html

    def _have_dot_number(self, dot_number):
        """
            when safer page is not _is_valid
            no reason to get insurance/license data
        """
        entry = self.manager.shard.query(SaferReport).filter(SaferReport.dot_number == self.dot_number).scalar()
        if entry is not None:
            return True
        else:
            return False
