import typer
from typing import Annotated
from datetime import datetime
import tabulate
from collections import defaultdict, OrderedDict

from . import common_args as ca
from .file_utils import find_log_files_in_date_range, read_files_reverse
from .log_utils import safe_parse_line
from .log_tools import DateRangedLogFile, LogFilteringConfig, FilterMode, value_matches

stats = typer.Typer()

def tabluate_log_matches(files: list[DateRangedLogFile], cfg: LogFilteringConfig):
    # Queue to hold lines ahead of/behind matches for printing
    matched_lines = 0

    non_partition_keys = [(k, v) for k,v in cfg.filter_list if k != cfg.partition_key]
    filter_counts: dict[str, dict[tuple[str,str], int]] = {
        files[0].first_record.get(cfg.partition_key): OrderedDict([
            [(k, v), 0] for k, v in non_partition_keys])
    }

    for line in read_files_reverse(files, cfg.chunk_size):
        parsed, fields = safe_parse_line(line)
        if not parsed:
            continue
        for k, v in non_partition_keys:
            if value_matches(fields.get(k), v, cfg.filter_mode):
                filter_counts[fields.get(cfg.partition_key)][(k, v)] += 1


        time = datetime.fromisoformat(fields[cfg.time_field])
        if cfg.done_iterating(matched_lines, time):
            break


    rows = [[k, *(c for c in v.values())] for k, v in filter_counts.items()]
    
    headers = [cfg.partition_key, *[f"{k}={v}" for k,v in non_partition_keys]]

    return rows, headers

    
    # print(tabulate.tabulate(rows, headers=headers))

@stats.callback(invoke_without_command=True)
def get_filter_match_stats(
        log_path: ca.LogPathOpt,
        start_date: ca.StartDateArg = datetime.min,
        since: ca.SinceArg = None,
        end_date: ca.EndDateArg = datetime.max,
        until: ca.UntilArg = None,
        time_field: ca.TimeFieldArg = ca.TIME_FIELD,
        msg_field: ca.MsgFieldArg = ca.MSG_FIELD,
        max_lines: ca.MaxLinesArg = 0,
        chunk_size: ca.ChunkSizeArg = ca.CHUNK_SIZE,
        exclude_keys: ca.ExcludeKeysArg = ca.EXCLUDE_KEYS,
        partition_key: ca.PartitionKeyArg = "",
        filters: Annotated[list[str], typer.Option("-f", "--filters", help="Key-Value pairs that should appear in the logs")] = [],
        filter_mode: Annotated[FilterMode, typer.Option("-m", "--filter-mode", help="String comparison mode to use for filtering logs")] = FilterMode.RAW.value,
):
    """ Tabulate the count of matching filters in log messages across a partition key
    """

    filter_config = LogFilteringConfig(
        start_date, 
        since,
        end_date, 
        until,
        time_field, 
        msg_field, 
        max_lines, 
        chunk_size, 
        exclude_keys, 
        partition_key, 
        filters, 
        filter_mode)

    all_rows = []
    headers = []
    # Glob plain and compressed files from the input directory
    for _, files in find_log_files_in_date_range(log_path, start_date, end_date, time_field, partition_key):
        
        # Skip over files where the partition key (assumed to be the same for each record in a given file) doesn't
        # match a filter
        fields = files[0].first_record
        partition_filters = [v for k, v in filter_config.filter_list if k == partition_key]
        if not all(value_matches(fields[partition_key], v, filter_mode) for v in partition_filters):
            continue

        rows, headers = tabluate_log_matches(files, filter_config)
        all_rows += rows

    print(tabulate.tabulate(all_rows, headers=headers))
