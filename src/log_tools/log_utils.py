import json
import typing
from datetime import datetime


def safe_parse_line(line: str) -> tuple[bool, dict[str, typing.Any]]:
    """ Attempt to parse a line as JSON, logging an error and returning false
    if parsing fails
    """
    if not line:
        return False, {}
    try: 
        return True, json.loads(line)
    except json.JSONDecodeError as e:
        print(f"Unable to JSON-decode formatted line '{line}'")
        return False, {}

def dt_in_range_fix_tz(start_date: datetime, date: datetime, end_date: datetime):
    """
    Check whether a given date falls within a start and stop date, applying the
    date's tz to the range endpoints if they do not yet have them
    """
    start_date = start_date.replace(tzinfo=date.tzinfo)
    end_date = end_date.replace(tzinfo=date.tzinfo)

    return start_date <= date <= end_date

def dt_less_than_fix_tz(start_date: datetime, date: datetime)