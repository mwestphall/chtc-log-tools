import json
import typing
from datetime import datetime, timedelta


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
    start_date = start_date.replace(tzinfo=start_date.tzinfo or date.tzinfo)
    end_date = end_date.replace(tzinfo=start_date.tzinfo or date.tzinfo)

    return start_date <= date <= end_date

def compare_dts_fix_tz(date1: datetime, date2: datetime):
    """
    Compare two dates, merging their timezones if one is timezone-less
    """
    date1_tz = date1.replace(tzinfo = date1.tzinfo or date2.tzinfo)
    date2_tz = date2.replace(tzinfo = date2.tzinfo or date1.tzinfo)

    return date1_tz - date2_tz


def done_iterating(idx: int, max_lines: int, time: datetime, start_time: datetime, time_window: int = 10):
    """
    Util function to check whether the script should stop iterating over log lines
    given an index and an earliest log date
    """
    return ((max_lines and idx > max_lines) or
        compare_dts_fix_tz(start_time, time) > timedelta(minutes=time_window))