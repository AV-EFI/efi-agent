import logging
import logging.config
import os
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
@click.argument('input_file', type=click.Path(dir_okay=False, exists=True))
def push(
        input_file, journal=None, profile=None, prefix=None, suffix=None):
    """Push AVefi records to the handle system, updating or creating PIDs."""
    try:
        api = api_client.EpicApi(profile, prefix, suffix=suffix)
        scheduler = task_manager.Scheduler(api, journal, input_file=input_file)
        scheduler.submit()
    except Exception:
        log.exception('Could not handle the following exception:')
        sys.exit(1)
