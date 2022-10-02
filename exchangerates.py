import logging
from datetime import date
from logging.config import fileConfig

from dotenv import load_dotenv

from wired_exchange.exchange_rates.ExchangeRatesClient import ExchangeRatesClient

if __name__ == "__main__":
    logging.config.fileConfig('logging.conf')

    logger = logging.getLogger('main')
    logger.info('--------------------- starting Wired Exchange ---------------------')
    load_dotenv()
    with ExchangeRatesClient()as change:
        # balances = change.get_live_rate('CHF', 'USDT')
        balances = change.get_rate('BTC', ['CHF', 'EUR'], date(2012, 12, 31))
    print(balances)
    logger.info('--------------------- Wired Exchange Ended ---------------------')
