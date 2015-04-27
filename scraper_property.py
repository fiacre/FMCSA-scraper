### base module

import re
import lxml


class ScraperProperty:
    """
        descriptor
        @path_expr: xpath to entity
        @fields: field names to create dict

        use htmlEntity root returned by xpath
        recurse though children
        return child.text if not None
    """
    def __init__(self, expr, fields=[]):
        self.expr = expr
        self.fields = fields

    def __get__(self, instance, cls):
        node = instance.root.xpath(self.expr)
        assert(node is not None)

        def get_text(element, text_list):
            """
                recurse down this ElementTree node:
                either get a node with text
                or a text node (represented as _ElementUnicodeResult by etree)
            """
            if type(element) == lxml.etree._ElementUnicodeResult:
                text_list.append(element)
            else:
                for child in element.getchildren():
                    get_text(child, text_list)

                if element.text is not None:
                    text_list.append(element.text)

        text_list = []
        if hasattr(node, 'text') and node.text is not None:
            text_list.append(node.text)
        else:
            for elem in node:
                get_text(elem, text_list)

        # there are sometimes 2 text elements in a node
        # more than 3 is surely a problem
        if len(text_list) > 3:
            raise ValueError("Received more than two values for {0} text".format(node))

        try:
            return ' '.join(t for t in text_list)
        except:
            return None

    def __set__(self, instance, value):
        raise AttributeError("Cannot set this Scraper property")


class ScraperListProperty(ScraperProperty):
    def __get__(self, instance, instance_type=None):
        """
            safer uses 'X' next to cargo carried
            search for 'X'
            iterate through siblings (in td)
            to get string for cargo
        """
        node = instance.root.xpath(self.expr)
        data = []
        for td in node[0].xpath('.//td[@class="queryfield"][text()="X"]'):
            for field in td.itersiblings():
                if field.text and field.text != 'X':
                    data.append(field.text)
                for item in field.xpath('.//font'):
                    data.append(item.text)
        return data

    def __set__(self, instance, value):
        raise AttributeError("Cannot set this ScraperList property")


class ScraperDateProperty(ScraperProperty):
    def __get__(self, instance, cls=None):
        """
            be sure we are getting a date
            or return None
        """
        date_tag = super().__get__(instance, cls=None)  # Let super class do recursion
        pat = re.compile(r'(\d{2}/\d{2}/\d{4})', re.MULTILINE)
        if date_tag and type(date_tag) == str:
            m = pat.search(date_tag)
            if m:
                return m.group(1)
            else:
                return None

    def __set__(self, instance, value):
        raise AttributeError("Cannot set this ScraperDate property")


class ScraperTableProperty(ScraperProperty):
    def __get__(self, instance, cls=None):
        """ specific to insurance sub pages
            data in tr/center/font
        """
        table = instance.root.xpath(self.expr)

        def get_text(element):
            font = element.xpath('.//font')
            if font and font != []:
                for f in font:
                    if f.text is not None:
                        yield(f.text)
                    else:
                        yield(" ")
            else:
                yield(" ")

        rows = table[0].xpath('tr[@align="LEFT"]')
        l = []
        for row in rows:
            l.append(dict(zip(self.fields, [text for text in get_text(row)])))
        return l


class ScraperRowProperty(ScraperProperty):
    def __get__(self, instance, cls=None):
        table = instance.root.xpath(self.expr)
        assert(table is not None)

        def get_text(element, text_list):
            """
                similar to recursive method above
                keep one table's row of text in a list
            """
            if type(element) == lxml.etree._ElementUnicodeResult:
                text_list.append(str(element))
            else:
                for child in element.getchildren():
                    get_text(child, text_list)

                if element.text is not None:
                    m = re.match(r'^([ -~]+)$', element.text)
                    if m:
                        text_list.append(m.group(1))
                    if element.tag == 'td' and element.text == '':
                        text_list.append(' ')
                        #get_text(element, text_list)

            return text_list

        # Get the text list:
        # zip it up with fields passed in constructor
        # to create a dictionary
        # one dictionary per row of table
        rows = []
        data = {}
        for tr in table[0].getchildren():
            if tr.tag == 'tr' and tr.attrib.get('align') == 'LEFT':
                text_list = get_text(tr, [])
                data = dict(zip(self.fields, text_list))
                rows.append(data)
        return rows

    def __set__(self, instance, value):
        raise AttributeError("Cannot set this ScraperRows property")


class ScraperRevocationProperty(ScraperProperty):
    """
        Special property for revocation pages
        all text is inside center>font
        if a cell in the scraped table
        is empty, that cell is either a th or td
        and has no center tag inside
    """
    def __get__(self, instance, cls=None):
        table = instance.root.xpath(self.expr)

        def get_text(row):
            for td in row.itersiblings():
                for child in td.getchildren():
                    grandchildren = child.getchildren()
                    if grandchildren != []:
                        for grandchild in grandchildren:
                            if grandchild.tag == 'center':
                                font = child.xpath('.//font')
                                yield(font[0].text)
                    else:
                        yield(" ")

        rows = table[0].xpath('tr[@align="LEFT"]')
        l = []
        for row in rows:
            d = dict(zip(self.fields, [text for text in get_text(row)]))
            if d != {}:
                l.append(d)
            #l.append(dict(zip(self.fields, [text for text in get_text(row)])))
        return l
