import typer

from .log_tools import filter_logs_by_date

app = typer.Typer()
app.command()(filter_logs_by_date)


if __name__ == '__main__':
    app()