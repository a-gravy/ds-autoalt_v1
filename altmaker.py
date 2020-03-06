"""altmaker

Usage:
    altmaker.py business
    altmaker.py cbf

Options:
    -h --help Show this screen
    --version

"""
import os, logging
from docopt import docopt
from dstools.logging import setup_logging


setup_logging()
logger = logging.getLogger(__name__)


def main():
    arguments = docopt(__doc__, version='0.9.0')
    logging.info(arguments)


if __name__ == '__main__':
    main()
