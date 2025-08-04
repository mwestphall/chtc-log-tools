import typing
import re
import pytz
import msgspec
from datetime import datetime, timedelta, timezone
from .common_args import TIME_FIELD, MSG_FIELD, DISPLAY_TZ


def safe_parse_line(line: str, time_key: str) -> tuple[bool, dict[str, typing.Any]]:
    """ Attempt to parse a line as JSON, logging an error and returning false
    if parsing fails
    """
    if not line:
        return False, {}
    try: 
        # fluentd records are tab-delimited, typically the JSON body will be the last field
        fields = msgspec.json.decode(line.split('\t')[-1])
        fields[time_key] = datetime.fromisoformat(fields[time_key]).replace(tzinfo=timezone.utc)
        if not time_key in fields:
            return False, None
        return True, fields
    except msgspec.DecodeError as e:
        print(f"Unable to JSON-decode formatted line '{line}'")
        return False, {}

def dt_in_range_fix_tz(start_date: datetime, date: datetime, end_date: datetime):
    """
    Check whether a given date falls within a start and stop date, applying the
    date's tz to the range endpoints if they do not yet have them
    """
    return start_date <= date <= end_date

def compare_dts_fix_tz(date1: datetime, date2: datetime):
    """
    Compare two dates, merging their timezones if one is timezone-less
    """
    return date1 - date2

def convert_log_tz(date: datetime):
    return date.astimezone(DISPLAY_TZ)



def done_iterating(idx: int, max_lines: int, time: datetime, start_time: datetime, time_window: int = 10):
    """
    Util function to check whether the script should stop iterating over log lines
    given an index and an earliest log date
    """
    return ((max_lines and idx > max_lines) or
        compare_dts_fix_tz(start_time, time) > timedelta(minutes=time_window))


COLOR_CODES = {
    "DEBUG": "\033[36m", # Cyan
    "INFO":  "\033[32m", # Green
    "WARN":  "\033[33m", # Yellow
    "ERROR": "\033[31m", # Red
    "FATAL": "\033[31m", # Also Red
    "PARTITION": "\033[35m", # Magenta
    "TIME": "\033[94m", # Bright Blue
    "RESET": "\033[0m" # Unset
}

LEVEL_RE = re.compile(r'^(DEBUG|INFO|WARN|ERROR|FATAL)[: ]*(.*)')

def pretty_print(
        log_json: dict[str, typing.Any],
        time_key: str = TIME_FIELD, 
        msg_key: str = MSG_FIELD, 
        partition_key: str = "", 
        exclude_keys: str = ""):

    # If level is not explicitly set, try to get it from the log message
    if not log_json.get('level') and (msg_match := LEVEL_RE.match(log_json[msg_key])):
        log_json['level'] = msg_match[1]
        log_json[msg_key] = msg_match[2]

    start_code = COLOR_CODES[log_json.get("level", "INFO")]
    reset_code = COLOR_CODES["RESET"]
    date_string = convert_log_tz(log_json.get(time_key)).strftime("%H:%M:%S")


    print(f"   {COLOR_CODES['TIME']}{date_string}{reset_code} ", end="")
    print(f"{start_code}{log_json.get('level', 'INFO'):5}{reset_code} ", end="")
    print(f"{log_json.get(msg_key)} ", end="")


    extra_attrs = [f"{k}={v}" for k, v in log_json.items() if k not in [time_key, msg_key, partition_key, 'level', *exclude_keys.split(",")]]
    if extra_attrs:
        print(f"[{', '.join(extra_attrs)}]", end="")


    print("") # Newline


def print_partition_header(log_json: dict[str, typing.Any], time_key: str = TIME_FIELD, partition_key: str = ""):    
    date_string = log_json.get(time_key).strftime("%Y-%m-%d")
    print(f"\n[{COLOR_CODES['TIME']}{date_string} {COLOR_CODES['PARTITION']}{log_json[partition_key]}{COLOR_CODES['RESET']}]")
