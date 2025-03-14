import typer
from typing import Annotated
import typing as t
from pathlib import Path
from datetime import datetime
import json
from . import common_args as ca
from .utils import readlines_reverse
from .log_tools import dt_in_range_fix_tz

sequence = typer.Typer(help="Sub-commands to validate log sequence numbers")


@sequence.command("list")
def list_sequences(
    log_path: ca.LogPathOpt,
    start_date: ca.StartDateArg = datetime.min,
    end_date: ca.EndDateArg = datetime.max,
    time_field: ca.TimeFieldArg = 'time',
    max_lines: ca.MaxLinesArg = 0,
):
    """ Given a set of log files - return the unique logger IDs appearing 
    in those files 
    """
    pass



@sequence.command("check")
def check_sequence(
    log_path: ca.LogPathOpt,
    logger_id: Annotated[str, typer.Argument(help="Logger ID to verify")],
    start_date: ca.StartDateArg = datetime.min,
    end_date: ca.EndDateArg = datetime.max,
    time_field: ca.TimeFieldArg = 'time',
    max_lines: ca.MaxLinesArg = 0,
):
    """ Given a sequence ID appearing in a set of log files 
    - return any gaps in that logger's sequence
    """

    all_ids = []
    with open(log_path, 'rb') as logf:
        for idx, line in enumerate(readlines_reverse(logf)):
            if not line:
                continue
            try:
                fields :dict[str, t.Any]= json.loads(line)
            except json.JSONDecodeError as e:
                print(f"UNABLE TO PARSE LINE: '''\n\t{line}'''")
                continue

            time = datetime.fromisoformat(fields[time_field])
            if dt_in_range_fix_tz(start_date, time, end_date) and fields["sequence_info"]["logger_id"] == logger_id:
                all_ids.append(fields["sequence_info"]["sequence_no"])

            if max_lines and idx >= max_lines:
                break
    all_ids.sort()
    diffs = [l2 - l1 for l1, l2 in zip(all_ids, all_ids[1:])]
    missing_idx = [i for i, v in enumerate(diffs) if v > 1]
    start_idx = all_ids[0]
    end_idx = all_ids[-1]
    if not missing_idx:
        print(f"All log IDs present between {start_idx} and {end_idx}")
    for idx in missing_idx:
        gap = diffs[idx]
        val = all_ids[idx]
        print(f"Missing {gap - 1} logs after sequence {val}")
