import json
import pathlib

import click

from .cli import cli_main


@cli_main.command()
@click.argument(
    'dest_dir', type=click.Path(file_okay=False, writable=True))
@click.argument(
    'input_files', nargs=-1, type=click.Path(dir_okay=False, exists=True))
def move_out(dest_dir, input_files):
    """Sift through files and move some bogus ones out of the way.

    Check files for problems and move them to different directory if
    applicable. Currently, a simple check for activities without
    agents is made.

    """
    dir_path = pathlib.Path(dest_dir)
    for input_file in input_files:
        file_path = pathlib.Path(input_file)
        with file_path.open() as f:
            contents = json.load(f)
        if not isinstance(contents, list):
            contents = [contents]
        if missing_agent(contents):
            file_path.rename(dir_path / file_path.name)
            print(f"Moved out: {file_path}")


def missing_agent(input):
    for entry in input:
        events = entry.get('has_event', [])
        for event in events:
            activities = event.get('has_activity', [])
            for activity in activities:
                if not activity.get('has_agent'):
                    return True
    return False
