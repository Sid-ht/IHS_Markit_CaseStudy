"""
logging
~~~~~~~
This module contains a logging object instantiated
with setting config level to INFO, enabling logging for PySpark.
"""

import logging
from logging import Logger

logger: Logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logging.basicConfig(level=logging.INFO)
