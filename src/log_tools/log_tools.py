import typer
from typing import Annotated
from datetime import datetime
from . import common_args as ca
from .log_utils import safe_parse_line, dt_in_range_fix_tz
from .file_utils import  aggregate_log_files

filterer = typer.Typer()


@filterer.callback(invoke_without_command=True)
def filter_logs_by_date(
        log_path: ca.LogPathOpt,
        start_date: ca.StartDateArg = datetime.min,
        end_date: ca.EndDateArg = datetime.max,
        time_field: ca.TimeFieldArg = 'time',
        max_lines: ca.MaxLinesArg = 0,
        filters: Annotated[list[str], typer.Option("-f", "--filters", help="Key-Value pairs that should appear in the logs")] = []
):
    """ Reference function that parses newline-delimited, JSON formatted 
    logs based on a time range
    """

    # Parse a list of key, value pairs out of filters (assumed to be a list of "key=value" strings)
    # TODO input validation
    filter_list : dict[str, str] = dict(f.split("=") for f in filters)

    # TODO a real implementation of log filtering
    for idx, line in enumerate(aggregate_log_files(log_path, start_date, end_date, time_field)):
        parsed, fields = safe_parse_line(line)
        if not parsed:
            continue

        time = datetime.fromisoformat(fields[time_field])
        if dt_in_range_fix_tz(start_date, time, end_date) and all(fields.get(k) == v for k, v in filter_list.items()):
            print(line)

        if max_lines and idx >= max_lines:
            break

