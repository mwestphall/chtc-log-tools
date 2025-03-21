import typer
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
from typing import Annotated

load_dotenv()

TIME_FIELD = "time"
# Want to read a lot of the file into memory at once since decompression is expensive time-wise
CHUNK_SIZE = 4 * 1024 * 1024

LogPathOpt = Annotated[Path, typer.Argument(help="Path to the log file(s) to parse")]
StartDateArg = Annotated[datetime, typer.Option(help="First date/time from which to return logs")]
EndDateArg = Annotated[datetime, typer.Option(help="Last date/time from which to return logs")]
MaxLinesArg = Annotated[int, typer.Option(help="Max number of lines to seek backwards")]
TimeFieldArg = Annotated[str, typer.Option(help="Structured log field to parse timestamps from", envvar="TIME_FIELD")]
ChunkSizeArg = Annotated[int, typer.Option(help="Maximum chunk size of a file to read at once", envvar="CHUNK_SIZE")]