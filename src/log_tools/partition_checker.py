import typer
from typing import Annotated
from datetime import datetime
from enum import Enum
import re
from thefuzz import fuzz
import tabulate

from . import common_args as ca
from .file_utils import find_log_files_in_date_range, read_file_reverse, safe_parse_line
from .log_tools import LogFilteringConfig, value_matches, FilterMode

partition_checker = typer.Typer()

@partition_checker.callback(invoke_without_command=True)
def check_log_partitions(
        log_path: ca.LogPathOpt,
        start_date: ca.StartDateArg = ca.DT_BUFFERED_MIN,
        since: ca.SinceArg = None,
        end_date: ca.EndDateArg = ca.DT_BUFFERED_MAX,
        until: ca.UntilArg = None,
        time_field: ca.TimeFieldArg = ca.TIME_FIELD,
        partition_key: ca.PartitionKeyArg = "",
        filters: Annotated[list[str], typer.Option("-f", "--filters", help="Key-Value pairs that should appear in the logs")] = [],
        filter_mode: Annotated[FilterMode, typer.Option("-m", "--filter-mode", help="String comparison mode to use for filtering logs")] = FilterMode.RAW.value,
):
    """ Find the time ranges for which a given set of log partitions have records
    """

    # Parse a list of key, value pairs out of filters (assumed to be a list of "key=value" strings)
    filter_config = LogFilteringConfig(
        start_date, 
        since,
        end_date, 
        until,
        time_field, 
        None, 
        None, 
        None, 
        None, 
        partition_key, 
        filters, 
        filter_mode)

    filter_list : dict[str, str] = dict(f.split("=") for f in filters)

    rows: list[tuple[str, str, str]] = []
    for _, files in find_log_files_in_date_range(log_path, filter_config.start_time, filter_config.end_time, time_field, partition_key):
        fields = files[-1].first_record
        if not all(value_matches(fields.get(k), f, filter_mode) for k, f in filter_list.items()):
            continue
        start_time = files[-1].start_time
        end_time = files[0].end_time

        rows.append((fields[partition_key], start_time, end_time))

    print(tabulate.tabulate(rows, headers=[partition_key.title(), "First Record", "Last Record"], tablefmt='rounded_outline'))

