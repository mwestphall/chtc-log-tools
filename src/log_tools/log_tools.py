import typer
from typing import Annotated, Any
from datetime import datetime, timedelta
from enum import Enum
import re
from thefuzz import fuzz
import sys
import pytz
from collections import deque
from dataclasses import dataclass

from . import common_args as ca
from .log_utils import safe_parse_line, dt_in_range_fix_tz, done_iterating, pretty_print, print_partition_header, convert_log_tz
from .file_utils import find_log_files_in_date_range, read_files_reverse, DateRangedLogFile

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

class RotatingDequeue(deque):
    capacity: int
    def __init__(self, capacity: int):
        self.capacity = capacity

    def append(self, x):
        super().append(x)
        if len(self) > self.capacity:
            self.popleft()

@dataclass 
class PrintedPartition:
    partition: str
    date: datetime

    @property
    def printed_date(self) -> datetime:
        return self.date.strftime("%Y-%m-%d")

    def __eq__(self, value: "PrintedPartition"):
        return self.partition == value.partition and self.printed_date == value.printed_date

@dataclass
class LogFilteringConfig:
    # Required/common parameters
    start_date: datetime
    since: int
    end_date: datetime
    until: int
    time_field: str
    msg_field: str
    max_lines: int
    chunk_size: int
    exclude_keys: str
    partition_key: str
    filters: list[str]
    filter_mode: FilterMode

    # Optional parameters
    context_window: int = 0
    _from: str = ""
    _to: str = ""

    # Stateful item to track when new header metadata needs to be printed
    last_header: PrintedPartition = None
    # Stateful item to track when "now" is for relative times
    _now: datetime = None



    @property
    def now(self):
        if self._now is None:
            self._now = datetime.now()
        return self._now

    @property
    def filter_list(self):
        """ Parse a list of key, value pairs out of filters (assumed to be a list of "key=value" strings)
        """
        return [f.split("=", 1) for f in self.filters]


    @property
    def start_time(self):
        """ Return the absolute or relative start time for this config, depending on whether --since is set
        """
        return self.now - timedelta(hours=self.since) if self.since else ca.DISPLAY_TZ.localize(self.start_date)

    @property
    def end_time(self):
        """ Return the absolute or relative start time for this config, depending on whether --since is set
        """
        return self.now - timedelta(hours=self.until) if self.until else ca.DISPLAY_TZ.localize(self.end_date)

    def pretty_print(self, fields: dict[str, Any]):
        line_header = PrintedPartition(fields[self.partition_key], fields[self.time_field])
        if self.last_header is None or self.last_header != line_header:
            print_partition_header(fields, self.time_field, self.partition_key)

        pretty_print(fields, self.time_field, self.msg_field, self.partition_key, self.exclude_keys)

        self.last_header = line_header

    def done_iterating(self, matched_lines: int, time: datetime):
        return done_iterating(matched_lines, self.max_lines, time, self.start_time)

    def dt_in_range(self, time: datetime):
        return dt_in_range_fix_tz(self.start_time, time, self.end_time)

    def fields_match_filters(self, fields: dict[str, Any]):
        return (not self.filter_list) or (value_matches(fields.get(k), f, self.filter_mode) for k, f in self.filter_list)

@dataclass
class ContextWindow:
    """ Utility class for tracking whether or not the log stream is "inside" a
    printable window 
    """
    _from: str
    _to: str
    filter_mode: FilterMode
       
    # state variable
    in_context = None


    def update_context(self, fields: dict[str, Any]) -> tuple[bool, bool]:
        if self.in_context is None:
            # If there's no start/stop filter, the log stream is always in context
            self.in_context = not (self._from and self._to)

        if self._from and not self.in_context:
            field, _filter = self._from.split('=', 1)
            self.in_context = value_matches(fields.get(field), _filter, self.filter_mode)
        elif self._to and self.in_context:
            field, _filter = self._to.split('=', 1)
            self.in_context = not value_matches(fields.get(field), _filter, self.filter_mode)
            if not self.in_context:
                return (False, True)
        
        return self.in_context, False


def print_partitioned_log_files(files: list[DateRangedLogFile], cfg: LogFilteringConfig):
    # Queue to hold lines ahead of/behind matches for printing
    leading_lines : deque[str] = RotatingDequeue(cfg.context_window)
    trailing_line_count = 0
    matched_lines = 0

    context_window = ContextWindow(cfg._from, cfg._to, cfg.filter_mode)

    for line in read_files_reverse(files, cfg.chunk_size):
        parsed, fields = safe_parse_line(line, cfg.time_field)
        if not parsed:
            continue

        time = fields[cfg.time_field]
        if cfg.dt_in_range(time):
            in_context, done = context_window.update_context(fields)
            if done:
                cfg.pretty_print(fields)
                break
            elif not in_context:
                continue

        if cfg.dt_in_range(time) and all(cfg.fields_match_filters(fields)):
            if len(leading_lines):
                print('   ...')
            for field in [*leading_lines, fields]:
                cfg.pretty_print(field)
            leading_lines.clear()
            trailing_line_count = cfg.context_window
            matched_lines += 1
        elif trailing_line_count > 0:
            cfg.pretty_print(fields)
            trailing_line_count -= 1
        else:
            leading_lines.append(fields)

        if trailing_line_count == 0 and cfg.done_iterating(matched_lines, time):
            break

@filterer.callback(invoke_without_command=True)
def filter_logs_by_date(
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
        context_window: Annotated[int, typer.Option("-C", "--context", help="Number of context lines surrounding filter matches to show")] = 0,
        _from: Annotated[str, typer.Option("--from", help="Log pattern from which to start displaying lines")] = 0,
        _to: Annotated[str, typer.Option("--to", help="Log pattern from which to stop displaying lines")] = 0,
):
    """ Parse a set of newline-delimited, JSON formatted log files, printing 
    log messages that match both the specified set of text filters and
    date ranges.
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
        filter_mode, 
        context_window,
        _from,
        _to)

    # Glob plain and compressed files from the input directory
    for _, files in find_log_files_in_date_range(log_path, filter_config.start_time, filter_config.end_time, time_field, partition_key):
        
        # Skip over files where the partition key (assumed to be the same for each record in a given file) doesn't
        # match a filter
        fields = files[0].first_record
        partition_filters = [v for k, v in filter_config.filter_list if k == partition_key]
        if not all(value_matches(fields[partition_key], v, filter_mode) for v in partition_filters):
            continue

        print_partitioned_log_files(files, filter_config)

        

