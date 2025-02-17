# -*- coding: utf-8 -*-

"""
Python IPC (Inter-Process Communication) Wrapper and Implementation
~~~~~~~~~~~~~~~~~~~

A basic IPC implementation for Python 3

:copyright: (c) 2020-2020 dovedevic
:license: MIT, see LICENSE for more details.

"""

__title__ = 'ipyc'
__author__ = 'dovedevic'
__license__ = 'MIT'
__copyright__ = 'Copyright 2020-present dovedevic'
__version__ = '1.1.1'

__path__ = __import__('pkgutil').extend_path(__path__, __name__)

from collections import namedtuple
import logging

from .asynchronous import AsyncIPyCHost, AsyncIPyCClient
from .links import AsyncIPyCLink

VersionInfo = namedtuple('VersionInfo', 'major minor micro releaselevel serial')
version_info = VersionInfo(major=1, minor=1, micro=1, releaselevel='release', serial=0)

logging.getLogger(__name__).addHandler(logging.NullHandler())
