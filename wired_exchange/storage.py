from sqlalchemy import create_engine
from sqlalchemy import MetaData

WIRED_EXCHANGE_DATABASE = 'wired_exchange.db'


def _get_currencies_tablename(platform: str, symbol: str):
    return f'CURRENCY_{platform}_{symbol}'


class WiredStorage:

    def __init__(self, profile: str):
        self.__db = None
        self.__metadata = None
        self.profile = profile

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self, echo: bool = False):
        if self.__db is None:
            self.__db = create_engine(f'sqlite:///{self.profile}_{WIRED_EXCHANGE_DATABASE}', echo=echo)
            self.__metadata = MetaData()
        return self

    def close(self):
        if self.__db is not None:
            self.__db.dispose()
            self.__metadata = None
            self.__db = None
        return self

    def save_transactions(self, tr):
        self.open()
        tr.to_sql('TRANSACTIONS', self.__db)

    def save_prices(self, prices):
        #table_name = _get_currencies_tablename(prices.iat[0, ''], )
        raise NotImplementedError('todo')
