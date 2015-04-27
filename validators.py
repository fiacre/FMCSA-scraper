import dateutil.parser
import re
from scraper.libs.validators import (
    ValidationError,
    Validator,
    ValidBoolean,
    ValidFloat,
    ValidInteger,
    ValidDateTime,
    ValidString
)


class ValidFMCSADate(ValidDateTime):
    def __call__(self, value):
        """
            @value: scraped data
            try to parse as date
        """
        if value is None:
            return value

        try:
            dt = dateutil.parser.parse(value)
        except Exception as E:
            raise ValidationError(self.msg.format(self.field_name, value)) from E

        return super().__call__(dt)


class ValidFMCSABoolean(ValidBoolean):
    def __call__(self, value):
        """
            @value: scraped data
            Y, YES -> True
            N, No -> False
        """
        if value is None or value == '':
            return super().__call__(False)

        if value.upper() == 'Y' or value.upper() == 'YES':
            return super().__call__(True)
        elif value.upper() == 'N' or value.upper() == 'NO':
            return super().__call__(False)
        else:
            raise ValidationError(self.msg.format(self.field_name, value))


class ValidFMCSAInteger(ValidInteger):
    def __call__(self, value):
        """
            @value: scraped data
            nix commas in string
        """
        if value is None or value == '':
            return super().__call__(None)
        value = value.replace(',', '')
        return super().__call__(value)


class ValidFMCSAFloat(ValidFloat):
    def __call__(self, value):
        """
            @value: scraped data
            return string that can be converted to float
            (no % sign)
        """
        if value is None or value == '':
            return super().__call__(None)
        elif type(value) == str:
            value = value.strip()
            value = value.replace('%', '')
            if value:
                return super().__call__(value)
            else:
                return None


class ValidFMCSACarrierOperation(Validator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not self.msg:
            self.msg = "{0} must be one of 'A', 'B' or 'C' but found `{1}`"

    def __call__(self, value):
        """
            @value: scraped data
            A, B or C used as shorthand
        """
        if value is None:
            return super().__call__(None)

        elif value == 'A':
            return 'interstate'
        elif value == 'B':
            return 'intrastate_hazmat'
        elif value == 'C':
            return 'intrastate_non_hazmat'
        else:
            raise ValidationError(self.msg.format(self.field_name, value))


class ValidFMCSAString(ValidString):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __call__(self, value):
        """
            @value: scraped data
            return cleaned data:
                no html entities,
                no leading or trailing whitespace
                no linebreaks
        """
        if value is None:
            return super().__call__(None)
        elif type(value) == str:
            value = value.strip()
            value = value.replace("\r", '')
            value = value.replace("\n", '')
            value = value.replace('\u00A0', '')
            value = value.replace('&nbsp;', '')
            value = re.sub(r'\s+', ' ', value)
            return super().__call__(value)

        else:
            raise ValidationError(self.msg.format(self.field_name, value))


class ValidFMCSAMoney(ValidFMCSAString):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __call__(self, value):
        """
            @value: scraped data
            return cleaned data:
                no asterisk at end
                as well as valid FMCSA string
        """
        if value is None:
            return super().__call__(None)
        elif type(value) == str:
            m = re.match(r'^(\$[\d\,]+)\**$', value)
            if m:
                return super().__call__(m.group(1))
            else:
                raise ValidationError(self.msg.format(self.field_name, value))
