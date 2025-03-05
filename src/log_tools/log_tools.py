import typer
from typing import Annotated
from datetime import datetime
from pathlib import Path
import pytz
import json
import typing as t

def filter_logs_by_date(
    log_path: Annotated[Path, typer.Argument(help="Path to the log file(s) to parse")],
    start_date: Annotated[datetime, typer.Option(help="First date/time from which to return logs")] = datetime.min,
    end_date: Annotated[datetime, typer.Option(help="Last date/time from which to return logs")] = datetime.max,
    time_field: Annotated[str, typer.Option(help="Structured log field to parse timestamps from")] = "time",
    filters: Annotated[list[str], typer.Option(help="Key-Value pairs that should appear in the logs")] = []
):
    """ Reference function that parses newline-delimited, JSON formatted 
    logs based on a time range
    """

    # Go logs by default export TZinfo, ensure our date ranges are TZ aware
    start_date = start_date.replace(tzinfo=pytz.UTC)
    end_date = end_date.replace(tzinfo=pytz.UTC)

    # Parse a list of key, value pairs out of filters (assumed to be a list of "key=value" strings)
    # TODO input validation
    filter_list : dict[str, str] = dict(f.split("=") for f in filters)

    # TODO a real implementation of log filtering
    with open(log_path, 'r') as logf:
        while line := logf.readline():
            fields :dict[str, t.Any]= json.loads(line)
            time = datetime.fromisoformat(fields[time_field])
            if start_date <= time <= end_date and all(fields.get(k) == v for k, v in filter_list.items()):
                print(line)

