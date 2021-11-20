import logging
import pytomlpp
import os.path

_logger = logging.getLogger(__name__)
resource_path = os.path.join(os.path.split(__file__)[0], "resources")


def __merge(a, b, path=None):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                __merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


with open(os.path.join(resource_path, 'wired_exchange.toml'), 'r') as cfg:
    _config = pytomlpp.load(cfg)

if os.path.exists('wired_exchange.toml'):
    with open('wired_exchange.toml', 'r') as cfg:
        __merge(_config, pytomlpp.load(cfg))


def _log_request(request):
    _logger.debug(request.headers)
    _logger.debug(f"Request event hook: {request.method} {request.url} - Waiting for response")


def log_response(response):
    request = response.request
    _logger.debug(f"Response event hook: {request.method} {request.url} - Status {response.status_code}")


def raise_on_4xx_5xx(response):
    response.raise_for_status()


def to_timestamp_in_seconds(dt) -> int:
    return int(round(dt.timestamp()))


def to_timestamp_in_milliseconds(dt) -> int:
    return int(round(dt.timestamp() * 1000))
