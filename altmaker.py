"""altmaker

Usage:
    altmaker.py business [new_arrival | trending | weekly_top]
    altmaker.py cbf

Options:
    -h --help Show this screen
    --version

"""
import os, logging
from docopt import docopt
from dstools.logging import setup_logging
from dstools.cli.parser import parse


setup_logging()
logger = logging.getLogger(__name__)


def main():
    arguments = docopt(__doc__, version='0.9.0')
    cmd, opts = parse(arguments)
    logger.info(f"Executing '{cmd}' with arguments {opts}")



if __name__ == '__main__':
    main()
