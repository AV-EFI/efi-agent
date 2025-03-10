import logging
import logging.config
import os
import sys

from . import push
from .cli import cli_main


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
