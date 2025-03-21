import typer
from typing import Annotated
from datetime import datetime
from enum import Enum
import re
from . import common_args as ca
from .log_utils import safe_parse_line, dt_in_range_fix_tz, done_iterating, pretty_print
from .file_utils import  aggregate_log_files
from thefuzz import fuzz
import sys

filterer = typer.Typer()

class FilterMode(Enum):
    RAW = "raw"
    REGEX = "regex"
    FUZZY = "fuzzy"


def value_matches(value: str, filter: str, mode: FilterMode):
    if not value:
        return False
    if mode == FilterMode.RAW:
        return filter.lower() in value.lower()
    elif mode == FilterMode.REGEX:
        return re.search(filter, value)
    else:
        # TODO does having a fixed threshold here make sense?
        return fuzz.partial_ratio(value.lower(), filter.lower()) > 75 

@filterer.callback(invoke_without_command=True)
def filter_logs_by_date(
        log_path: ca.LogPathOpt,
        start_date: ca.StartDateArg = datetime.min,
        end_date: ca.EndDateArg = datetime.max,
        time_field: ca.TimeFieldArg = ca.TIME_FIELD,
        max_lines: ca.MaxLinesArg = 0,
        chunk_size: ca.ChunkSizeArg = ca.CHUNK_SIZE,
        filters: Annotated[list[str], typer.Option("-f", "--filters", help="Key-Value pairs that should appear in the logs")] = [],
        filter_mode: Annotated[FilterMode, typer.Option("-m", "--filter-mode", help="String comparison mode to use for filtering logs")] = FilterMode.RAW.value
):
    """ Reference function that parses newline-delimited, JSON formatted 
    logs based on a time range
    """

    # Parse a list of key, value pairs out of filters (assumed to be a list of "key=value" strings)
    filter_list : dict[str, str] = dict(f.split("=") for f in filters)

    output_tty = sys.stdout.isatty()

    for idx, line in enumerate(aggregate_log_files(log_path, start_date, end_date, time_field, chunk_size)):
        parsed, fields = safe_parse_line(line)
        if not parsed:
            continue

        time = datetime.fromisoformat(fields[time_field])
        if dt_in_range_fix_tz(start_date, time, end_date) and all(value_matches(fields.get(k), f, filter_mode) for k, f in filter_list.items()):
            if output_tty:
                pretty_print(fields)
            else:
                print(line)

        if done_iterating(idx, max_lines, time, start_date):
            break

