import typer
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from datetime import datetime, timedelta
import pytz
from typing import Annotated
from os import environ

load_dotenv(find_dotenv(usecwd=True))

TIME_FIELD = "time"
MSG_FIELD = "msg"
# Want to read a lot of the file into memory at once since decompression is expensive time-wise
CHUNK_SIZE = 4 * 1024 * 1024
EXCLUDE_KEYS = "level,sequence_info"

# TODO code paths for timezone substititution are too nested to easily pass as arg, make global derived from
# env instead
DISPLAY_TZ = pytz.timezone(environ.get('LOG_TIMEZONE', 'America/Chicago'))

# DateTime Min/Max with a buffer for timezone conversions
DT_BUFFERED_MIN = datetime.min + timedelta(days=365)
DT_BUFFERED_MAX = datetime.max - timedelta(days=365)


LogPathOpt = Annotated[list[Path], typer.Argument(help="Path to the log file(s) to parse")]
StartDateArg = Annotated[datetime, typer.Option(help="First date/time from which to return logs")]
EndDateArg = Annotated[datetime, typer.Option(help="Last date/time from which to return logs")]
SinceArg = Annotated[int, typer.Option(help="First date/time from which to return logs, relative to current time (in hours)")]
UntilArg = Annotated[int, typer.Option(help="Last date/time from which to return logs, relative to current time (in hours)")]
MaxLinesArg = Annotated[int, typer.Option(help="Max number of lines to seek backwards")]
TimeFieldArg = Annotated[str, typer.Option(help="Structured log field to parse timestamps from", envvar="TIME_KEY")]
MsgFieldArg = Annotated[str, typer.Option(help="Structured log field containing the main body of the message", envvar="MESSAGE_KEY")]
ExcludeKeysArg = Annotated[str, typer.Option(help="Comma-separated structured log fields to omit from output", envvar="EXCLUDE_KEYS")]
PartitionKeyArg = Annotated[str, typer.Option(help="Comma-separated fields on which logs are partitioned, in addition to time", envvar="PARTITION_KEYS")]
ChunkSizeArg = Annotated[int, typer.Option(help="Maximum chunk size of a file to read at once", envvar="CHUNK_SIZE")]
