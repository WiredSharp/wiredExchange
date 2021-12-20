import httpx
import pandas as pd

from wired_exchange.core.ExchangeClient import ExchangeClient
from wired_exchange.kucoin.KucoinAuthenticator import KucoinAuthenticator


class KucoinFuturesClient(ExchangeClient):

    def __init__(self, futures_api_key: str = None, futures_api_passphrase: str = None,
                 futures_api_secret: str = None, futures_host_url: str = None):
        super().__init__('kucoin_futures', futures_api_key, futures_api_secret, futures_host_url, always_authenticate=False)
        self._api_passphrase = futures_api_passphrase if futures_api_passphrase is not None else self._get_exchange_env_value(
            'api_passphrase')
        self._authenticator = KucoinAuthenticator(self._api_key, self._api_passphrase, self._api_secret)

    def _authenticate(self, request):
        self._authenticator.authenticate(request)

    def get_positions(self) -> pd.DataFrame:
        self.open()
        request = self._httpClient.build_request('GET', 'v1/positions')
        self._authenticate(request)
        try:
            json = self._httpClient.send(request).json()
            if not json['code'].startswith('200'):
                raise RuntimeError(f'{json["msg"]} ({json["code"]}): response code does not indicate a success')
            return self._to_positions(json)
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve futures positions from Kucoin') from ex

    def get_position(self, symbol:str):
        self.open()
        request = self._httpClient.build_request('GET', 'v1/positions', params={symbol: symbol})
        self._authenticate(request)
        try:
            json = self._httpClient.send(request).json()
            if not json['code'].startswith('200'):
                raise RuntimeError(f'{json["msg"]} ({json["code"]}): response code does not indicate a success')
            return self._to_positions(json)
        except httpx.HTTPStatusError as ex:
            raise RuntimeError('cannot retrieve futures positions from Kucoin') from ex

    def _to_positions(self, json):
        positions = pd.DataFrame(json['data'])
        positions.drop(columns=['id', 'maintMarginReq', 'riskLimit', 'crossMode', 'markValue', 'unrealisedRoePcnt', 'posMaint', 'maintMargin', 'realisedGrossPnl', 'posCross', 'delevPercentage', 'realisedGrossCost'], inplace=True)
        positions['realLeverage'] = pd.to_numeric(positions['realLeverage'])
        positions['currentQty'] = pd.to_numeric(positions['currentQty'])
        positions['currentCost'] = pd.to_numeric(positions['currentCost'])
        positions['currentComm'] = pd.to_numeric(positions['currentComm'])
        positions['unrealisedCost'] = pd.to_numeric(positions['unrealisedCost'])
        positions['realisedCost'] = pd.to_numeric(positions['realisedCost'])
        positions['markPrice'] = pd.to_numeric(positions['markPrice'])
        positions['posCost'] = pd.to_numeric(positions['posCost'])
        positions['posInit'] = pd.to_numeric(positions['posInit'])
        positions['posComm'] = pd.to_numeric(positions['posComm'])
        positions['posLoss'] = pd.to_numeric(positions['posLoss'])
        positions['posMargin'] = pd.to_numeric(positions['posMargin'])
        positions['realisedPnl'] = pd.to_numeric(positions['realisedPnl'])
        positions['unrealisedPnl'] = pd.to_numeric(positions['unrealisedPnl'])
        positions['avgEntryPrice'] = pd.to_numeric(positions['avgEntryPrice'])
        positions['liquidationPrice'] = pd.to_numeric(positions['liquidationPrice'])
        positions['bankruptPrice'] = pd.to_numeric(positions['bankruptPrice'])
        positions['unrealisedPnlPcnt'] = pd.to_numeric(positions['unrealisedPnlPcnt'])
        positions['openingTimestamp'] = pd.to_datetime(positions['openingTimestamp'], unit='ms')
        positions['currentTimestamp'] = pd.to_datetime(positions['currentTimestamp'], unit='ms')
        return positions


