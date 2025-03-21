import typer
from datetime import datetime
from . import common_args as ca
from .file_utils import aggregate_log_files
from .log_tools import dt_in_range_fix_tz
from .log_utils import safe_parse_line
from collections import defaultdict

sequence = typer.Typer(help="Sub-commands to validate log sequence numbers")

@sequence.callback(invoke_without_command=True)
def check_sequence(
    log_path: ca.LogPathOpt,
    start_date: ca.StartDateArg = datetime.min,
    end_date: ca.EndDateArg = datetime.max,
    time_field: ca.TimeFieldArg = 'time',
    max_lines: ca.MaxLinesArg = 0,
):
    """ Given a set of log files containing the special "sequence_info" JSON sub-object:
    {"sequence_info": {"logger_id": "<uuid>", "sequence_no": <int> }}
    return any gaps in the log sequences appearing in that file
    """

    # For each logger, record every log sequence appearing under its logger ID
    logger_ids : dict[str, list[int]]= defaultdict(list)
    for idx, line in enumerate(aggregate_log_files(log_path, start_date, end_date, time_field)):
        parsed, fields = safe_parse_line(line)
        if not parsed:
            continue

        time = datetime.fromisoformat(fields[time_field])
        if dt_in_range_fix_tz(start_date, time, end_date) and fields.get("sequence_info", dict()).get("logger_id"):
            logger_ids[fields["sequence_info"]["logger_id"]].append(fields["sequence_info"]["sequence_no"])

        if max_lines and idx >= max_lines:
            break

    # For each logger, compute any gaps in the logger's recorded sequence
    for logger_id, all_ids in logger_ids.items():
        all_ids.sort() # Sort the list of recorded IDs
        diffs = [l2 - l1 for l1, l2 in zip(all_ids, all_ids[1:])] # Compute gaps between log sequence #s
        missing_idx = [i for i, v in enumerate(diffs) if v > 1] # Find indices where gap > 1
        start_idx = all_ids[0]
        end_idx = all_ids[-1]
        print(f"Log sequence stats for logger {logger_id}")
        for idx in missing_idx:
            gap = diffs[idx]
            val = all_ids[idx]
            print(f"  Missing {gap - 1} logs after sequence {val}")
        else:
            print(f"  All log IDs present between {start_idx} and {end_idx}")
