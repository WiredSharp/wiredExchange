from datetime import datetime, timedelta
import logging
import tzlocal
from dotenv import load_dotenv
from wired_exchange.ftx.FTXClient import FTXClient

load_dotenv()

logging.basicConfig(filename='wired_exchange.log', encoding='utf-8', level=logging.DEBUG
                    , format='%(asctime)s [%(levelname)s] %(name)s - %(message)s')

logger = logging.getLogger('main')

logger.info('--------------------- starting Wired Exchange ---------------------')
with FTXClient() as client:
    fills = client.get_transactions(datetime.now(tzlocal.get_localzone()) + timedelta(days=-10))
    print(fills)
logger.info('--------------------- Wired Exchange stopped ---------------------')
