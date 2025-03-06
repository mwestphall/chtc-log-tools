import typer
from dataclasses import dataclass
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
from typing import Annotated

load_dotenv()

@dataclass
class CommonArgs:
    start_date: datetime
    end_date: datetime
    time_field: str

LogPathOpt = Annotated[Path, typer.Argument(help="Path to the log file(s) to parse")]

def common_args(
        ctx: typer.Context,
        start_date: Annotated[datetime, typer.Option(help="First date/time from which to return logs")] = datetime.min,
        end_date: Annotated[datetime, typer.Option(help="Last date/time from which to return logs")] = datetime.max,
        time_field: Annotated[str, typer.Option(help="Structured log field to parse timestamps from", envvar="TIME_FIELD")] = "time",
):
    """ Universal Args """
    ctx.obj = CommonArgs(start_date, end_date, time_field)