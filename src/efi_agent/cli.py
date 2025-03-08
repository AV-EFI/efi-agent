import json
import logging
import logging.config
import os
import pathlib
import sys

import click

from . import api_client, task_manager


log = logging.getLogger(__name__)
loglevel = os.environ.get(
    f"{__package__.upper()}_LOGLEVEL", 'INFO').upper()
logging_config = {
    'version': 1,
    'formatters': {
        'simple': {
            'format': '%(levelname)s %(name)s: %(message)s',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': loglevel,
            'formatter': 'simple',
            'stream': 'ext://sys.stderr',
        },
    },
    'loggers': {
        __package__: {
            'level': 'DEBUG',
            'handlers': ['console'],
        },
    },
    'disable_existing_loggers': False,
}
logging.config.dictConfig(logging_config)


@click.group()
def cli_main():
    pass


@cli_main.command()
@click.option(
    '-j', '--journal', type=click.Path(dir_okay=False, writable=True),
    help='Journal where actions on PIDs are to be recorded.')
@click.option(
    '-p', '--profile', type=click.Path(dir_okay=False, exists=True),
    help='Profile for the described_by slot in the AVefi schema.')
@click.option(
    '--prefix',
    help='Prefix to be used when generating handles.')
@click.option(
    '--suffix',
    help='Suffix to be used when generating handles.')
@click.argument(
    'input_files', nargs=-1, type=click.Path(dir_okay=False, exists=True))
def push(input_files, journal=None, profile=None, prefix=None, suffix=None):
    """Push AVefi records to the handle system, updating or creating PIDs."""
    journal_file = pathlib.Path(journal)
    result_log = []
    try:
        # Make sure we have write permissions and read present contents
        with journal_file.open('a+') as f:
            if f.tell() != 0:
                f.seek(0)
                result_log = json.load(f)
        api = api_client.EpicApi(profile, prefix, suffix=suffix)
        for input_file in input_files:
            try:
                scheduler = task_manager.Scheduler(
                    api, result_log, input_file=input_file)
                scheduler.submit()
            except task_manager.UnreferencedError as e:
                log.error(f"Skipped {input_file} due to incomplete data: {e}")
            except Exception:
                write_pid_journal(journal_file, result_log)
            else:
                write_pid_journal(journal_file, result_log)
    except Exception:
        log.exception('Could not handle the following exception:')
        sys.exit(1)


def write_pid_journal(journal_file, result_log):
    if result_log:
        with journal_file.open('w') as f:
            json.dump(result_log, f, indent=2)
            f.write('\n')
