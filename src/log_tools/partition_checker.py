import typer
from typing import Annotated
from datetime import datetime
from enum import Enum
import re
from thefuzz import fuzz
import tabulate

from . import common_args as ca
from .file_utils import find_log_files_in_date_range, read_file_reverse, safe_parse_line

partition_checker = typer.Typer()

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

@partition_checker.callback(invoke_without_command=True)
def check_log_partitions(
        log_path: ca.LogPathOpt,
        start_date: ca.StartDateArg = datetime.min,
        end_date: ca.EndDateArg = datetime.max,
        time_field: ca.TimeFieldArg = ca.TIME_FIELD,
        max_lines: ca.MaxLinesArg = 0,
        chunk_size: ca.ChunkSizeArg = ca.CHUNK_SIZE,
        partition_key: ca.PartitionKeyArg = "",
        filters: Annotated[list[str], typer.Option("-f", "--filters", help="Key-Value pairs that should appear in the logs")] = [],
        filter_mode: Annotated[FilterMode, typer.Option("-m", "--filter-mode", help="String comparison mode to use for filtering logs")] = FilterMode.RAW.value,
):
    """ Reference function that parses newline-delimited, JSON formatted 
    logs based on a time range
    """

    # Parse a list of key, value pairs out of filters (assumed to be a list of "key=value" strings)
    filter_list : dict[str, str] = dict(f.split("=") for f in filters)

    rows: list[tuple[str, str, str]] = []
    for _, files in find_log_files_in_date_range(log_path, start_date, end_date, time_field, partition_key):
        fields = files[0].first_record
        if not all(value_matches(fields.get(k), f, filter_mode) for k, f in filter_list.items()):
            continue
        start_time = fields[time_field]
        end_time = None
        for l in read_file_reverse(files[-1].path, chunk_size):
            parsed, fields = safe_parse_line(l)
            if not parsed:
                continue
            end_time = fields[time_field]
            break

        rows.append((fields[partition_key], start_time, end_time))

    print(tabulate.tabulate(rows, headers=[partition_key.title(), "First Record", "Last Record"]))

