import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, FLOAT, TIMESTAMP, NVARCHAR

from wired_exchange.core import to_transactions

WIRED_EXCHANGE_DATABASE = 'wired_exchange.db'
TRANSACTIONS_TABLE_NAME = 'TRANSACTIONS'

class WiredStorage:

    def __init__(self, profile: str):
        self.__db = None
        self.__metadata = None
        self.profile = profile.lower()

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self, echo: bool = False):
        if self.__db is None:
            self.__db = create_engine(f'sqlite:///{self.profile}_{WIRED_EXCHANGE_DATABASE}', echo=echo)
            self.__metadata = MetaData(self.__db)
            self.__metadata.reflect()
        return self

    def close(self):
        if self.__db is not None:
            self.__db.dispose()
            self.__metadata = None
            self.__db = None
        return self

    def save_transactions(self, tr):
        self.open()
        if TRANSACTIONS_TABLE_NAME not in self.__metadata.tables.keys():
            transactions = Table(TRANSACTIONS_TABLE_NAME, self.__metadata,
                                 Column('id', NVARCHAR(25), primary_key=True),
                                 Column('base_currency', NVARCHAR(5)),
                                 Column('quote_currency', NVARCHAR(5)),
                                 Column('type', NVARCHAR(25)),
                                 Column('side', NVARCHAR(25)),
                                 Column('price', FLOAT),
                                 Column('size', FLOAT),
                                 Column('order_id', NVARCHAR(25)),
                                 Column('time', TIMESTAMP),
                                 Column('trade_id', NVARCHAR(25)),
                                 Column('fee_rate', FLOAT),
                                 Column('fee', FLOAT),
                                 Column('fee_currency', NVARCHAR(5)),
                                 Column('platform', NVARCHAR(50)),
                                 Column('price_usd', FLOAT),
                                 Column('fee_usd', FLOAT)
                                 )
            transactions.create(self.__db)
        tr.to_sql('TRANSACTIONS', self.__db, method=_upsert, if_exists='append', index=True, index_label='id')

    def save_prices(self, prices):
        # table_name = _get_currencies_tablename(prices.iat[0, ''], )
        raise NotImplementedError('todo')

    def read_transactions(self):
        self.open()
        return pd.read_sql_table('TRANSACTIONS', self.__db, index_col='id',
                                 parse_dates=['time'])


def _get_unicode_name(name):
    try:
        uname = str(name).encode("utf-8", "strict").decode("utf-8")
    except UnicodeError as err:
        raise ValueError(f"Cannot convert identifier to UTF-8: '{name}'") from err
    return uname


def _get_valid_sqlite_name(name):
    # See https://stackoverflow.com/questions/6514274/how-do-you-escape-strings\
    # -for-sqlite-table-column-names-in-python
    # Ensure the string can be encoded as UTF-8.
    # Ensure the string does not include any NUL characters.
    # Replace all " with "".
    # Wrap the entire thing in double quotes.

    uname = _get_unicode_name(name)
    if not len(uname):
        raise ValueError("Empty table or column name specified")

    nul_index = uname.find("\x00")
    if nul_index >= 0:
        raise ValueError("SQLite identifier cannot contain NULs")
    return '"' + uname.replace('"', '""') + '"'


def _get_currencies_tablename(platform: str, symbol: str):
    return f'CURRENCY_{platform}_{symbol}'


def insert_statement(*, table, columns: list, num_rows: int):
    names = list(map(str, table.frame.columns))
    wld = "?"  # wildcard char
    escape = _get_valid_sqlite_name

    if table.index is not None:
        for idx in table.index[::-1]:
            names.insert(0, idx)

    bracketed_names = [escape(column) for column in names]
    col_names = ",".join(bracketed_names)

    row_wildcards = ",".join([wld] * len(names))
    wildcards = ",".join(f"({row_wildcards})" for _ in range(num_rows))
    insert_statement = (
        f"INSERT INTO {escape(table.name)} ({col_names}) VALUES {wildcards} ON CONFLICT DO NOTHING"
    )
    return insert_statement


def _upsert(table, cx, keys, data_iter):
    data_list = list(data_iter)
    flattened_data = [x for row in data_list for x in row]
    cx.execute(insert_statement(table=table, columns=keys, num_rows=len(data_list)), flattened_data)