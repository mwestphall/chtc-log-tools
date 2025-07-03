import typer
from typing import Annotated
from datetime import datetime
from enum import Enum
import re
from thefuzz import fuzz
import sys

from . import common_args as ca
from .log_utils import safe_parse_line, dt_in_range_fix_tz, done_iterating, pretty_print
from .file_utils import  aggregate_log_files, find_log_files_in_date_range, read_files_reverse

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
        msg_field: ca.MsgFieldArg = ca.MSG_FIELD,
        max_lines: ca.MaxLinesArg = 0,
        chunk_size: ca.ChunkSizeArg = ca.CHUNK_SIZE,
        exclude_keys: ca.ExcludeKeysArg = ca.EXCLUDE_KEYS,
        partition_key: ca.PartitionKeyArg = "",
        filters: Annotated[list[str], typer.Option("-f", "--filters", help="Key-Value pairs that should appear in the logs")] = [],
        filter_mode: Annotated[FilterMode, typer.Option("-m", "--filter-mode", help="String comparison mode to use for filtering logs")] = FilterMode.RAW.value,
        raw_output: Annotated[bool, typer.Option("--raw", help="Don't pretty-print logs")] = False,
):
    """ Reference function that parses newline-delimited, JSON formatted 
    logs based on a time range
    """

    # Parse a list of key, value pairs out of filters (assumed to be a list of "key=value" strings)
    filter_list : dict[str, str] = dict(f.split("=") for f in filters)

    output_tty = sys.stdout.isatty()

    for _, files in find_log_files_in_date_range(log_path, start_date, end_date, time_field, partition_key):
        fields = files[0].first_record

        if (partition_filter := filter_list.get(partition_key)) and not value_matches(fields[partition_key], partition_filter, filter_mode):
            continue

        matched_lines = 0
        for line in read_files_reverse(files, chunk_size):
            parsed, fields = safe_parse_line(line)
            if not parsed:
                continue

            time = datetime.fromisoformat(fields[time_field])
            if dt_in_range_fix_tz(start_date, time, end_date) and all(value_matches(fields.get(k), f, filter_mode) for k, f in filter_list.items()):
                if output_tty and not raw_output:
                    pretty_print(fields, time_field, msg_field, partition_key, exclude_keys)
                else:
                    print(line)
                matched_lines+=1

            if done_iterating(matched_lines, max_lines, time, start_date):
                break
        print('---')

