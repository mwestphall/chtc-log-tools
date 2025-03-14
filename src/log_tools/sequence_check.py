import typer
from typing import Annotated
from pathlib import Path
from datetime import datetime
from .common_args import LogPathOpt

sequence = typer.Typer(help="Sub-commands to validate log sequence numbers")


@sequence.command("list")
def list_sequences(
    ctx: typer.Context,
    log_path: LogPathOpt,
):
    """ Given a set of log files - return the unique logger IDs appearing 
    in those files 
    """
    pass



@sequence.command("check")
def check_sequence(
    ctx: typer.Context,
    log_path: LogPathOpt,
):
    """ Given a sequence ID appearing in a set of log files 
    - return any gaps in that logger's sequence
    """
    pass