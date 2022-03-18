import logging

import pytomlpp
import os.path

from datetime import datetime

from wired_exchange.ftx import FTXClient
from wired_exchange.kucoin import KucoinSpotClient
from wired_exchange.storage import WiredStorage
from wired_exchange.core import config, merge


if os.path.exists('wired_exchange.toml'):
    with open('wired_exchange.toml', 'r') as cfg:
        merge(config(), pytomlpp.load(cfg))