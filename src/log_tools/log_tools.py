import typer
from typing import Annotated
from datetime import datetime
from pathlib import Path
import pytz
import json
import typing as t
from .common_args import CommonArgs, LogPathOpt


def dt_in_range_fix_tz(start_date: datetime, date: datetime, end_date: datetime):
    """
    Check whether a given date falls within a start and stop date, applying the
    date's tz to the range endpoints if they do not yet have them
    """
    start_date = start_date.replace(tzinfo=date.tzinfo)
    end_date = end_date.replace(tzinfo=date.tzinfo)

    return start_date <= date <= end_date

def filter_logs_by_date(
        ctx: typer.Context,
        log_path: LogPathOpt,
        filters: Annotated[list[str], typer.Option(help="Key-Value pairs that should appear in the logs")] = []
):
    """ Reference function that parses newline-delimited, JSON formatted 
    logs based on a time range
    """

    args: CommonArgs = ctx.obj 
    # Parse a list of key, value pairs out of filters (assumed to be a list of "key=value" strings)
    # TODO input validation
    filter_list : dict[str, str] = dict(f.split("=") for f in filters)

    # TODO a real implementation of log filtering
    with open(log_path, 'r') as logf:
        while line := logf.readline():
            fields :dict[str, t.Any]= json.loads(line)
            time = datetime.fromisoformat(fields[args.time_field])
            if dt_in_range_fix_tz(args.start_date, time, args.end_date) and all(fields.get(k) == v for k, v in filter_list.items()):
                print(line)

