import logging
from datetime import datetime, timezone
from logging.config import fileConfig

import dateutil
from dotenv import load_dotenv

from wired_exchange.bitpandapro import BitPandaProClient

if __name__ == "__main__":
    logging.config.fileConfig('logging.conf')

    logger = logging.getLogger('bitpanda')
    logger.info('--------------------- starting Wired Exchange ---------------------')
    load_dotenv()
    # print(dateutil.parser.isoparse('2022-06-10T16:32:50Z'))
    # print(dateutil.parser.isoparse('2022-06-10T16:32:50Z').isoformat())
    with BitPandaProClient()as bp:
    #     orders = bp.get_orders(start_time=dateutil.parser.isoparse('2022-06-10T16:32:50Z'),
    #                            end_time=datetime.now(timezone.utc))
    # print(orders)
        balances = bp.get_balances()
    print(balances)
    logger.info('--------------------- Wired Exchange Ended ---------------------')
