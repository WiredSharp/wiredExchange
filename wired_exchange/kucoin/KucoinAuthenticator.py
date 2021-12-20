import base64
import hashlib
import hmac
import logging
import time

class KucoinAuthenticator:
    def __init__(self, api_key: str = None, api_passphrase: str = None,
                 api_secret: str = None):
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._api_key = api_key
        self._logger = logging.getLogger(type(self).__name__)

    def authenticate(self, request):
        ts = int(round(time.time())) * 1000
        self._logger.debug(f'timestamp {ts}')
        signature_payload = f'{str(ts)}{request.method.upper()}{request.url.raw_path.decode("utf-8")}'
        if request.content:
            signature_payload += request.content.decode('utf-8')
        signature = base64.b64encode(
            hmac.new(self._api_secret.encode('utf-8'), signature_payload.encode('utf-8'), hashlib.sha256).digest())
        self._logger.debug(f'payload {signature_payload}')
        passphrase = base64.b64encode(
            hmac.new(self._api_secret.encode('utf-8'), self._api_passphrase.encode('utf-8'),
                     hashlib.sha256).digest())
        request.headers['KC-API-SIGN'] = signature.decode('utf-8')
        request.headers['KC-API-TIMESTAMP'] = str(ts)
        request.headers['KC-API-KEY'] = self._api_key
        request.headers['KC-API-PASSPHRASE'] = passphrase.decode('utf-8')
        request.headers['KC-API-KEY-VERSION'] = "2"