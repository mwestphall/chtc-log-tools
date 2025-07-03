import typer

from .sequence_check import sequence
from .log_tools import filterer
from .partition_checker import partition_checker


app = typer.Typer()
app.add_typer(filterer, name="filter")
app.add_typer(partition_checker, name="check")
app.add_typer(sequence, name="sequence")



if __name__ == '__main__':
    app()
