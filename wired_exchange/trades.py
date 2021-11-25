from datetime import datetime

from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Trade(Base):
    __tablename__ = 'trades'

    # https://stackoverflow.com/questions/7165998/how-to-do-an-upsert-with-sqlalchemy
    # id = Column(, )
    def __init__(self, trade_id: str
                 , order_id: str
                 , platform: str
                 , base_currency: str
                 , quantity: float
                 , quote_currency: str
                 , price: float
                 , fee: float
                 , fee_currency: str
                 , side: str
                 , timestamp: datetime
                 , price_usd: float
                 , fee_usd: float
                 ):
        self.fee_usd = fee_usd
        self.price_usd = price_usd
        self.timestamp = timestamp
        self.side = side
        self.fee_currency = fee_currency
        self.fee = fee
        self.price = price
        self.quote_currency = quote_currency
        self.quantity = quantity
        self.base_currency = base_currency
        self.platform = platform
        self.trade_id = trade_id
        self.order_id = order_id
