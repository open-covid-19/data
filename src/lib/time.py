import datetime
from .cast import safe_datetime_parse


def datetime_isoformat(value: str, date_format: str) -> str:
    date = safe_datetime_parse(value, date_format)
    if date is not None:
        return date.date().isoformat()
    else:
        return None

def date_offset(value: str, offset: int) -> str:
    assert offset is not None, 'Offset none: %r' % offset
    value = datetime.date.fromisoformat(value)
    value += datetime.timedelta(days=offset)
    return value.isoformat()
