# crypto exchange BOT SDK

- [ ] Retrieves market prices form various exchange in a homogeneous way
  - [X] [FTX](https://docs.ftx.com/?python#rest-api)
  - [X] [Kucoin](https://docs.kucoin.com/#general)
  - [ ] [Binance](https://binance-docs.github.io/apidocs)
  - [ ] [BitPanda](https://developers.bitpanda.com/platform/)
- [ ] Retrieve portfolio information
  - [X] transactions history
    - [X] [FTX](https://docs.ftx.com/?python#rest-api)
    - [X] [Kucoin](https://docs.kucoin.com/#general)
    - [ ] [Binance](https://binance-docs.github.io/apidocs)
    - [ ] [BitPanda](https://developers.bitpanda.com/platform/)
  - [ ] Open orders
    - [ ] [FTX](https://docs.ftx.com/?python#rest-api)
    - [ ] [Kucoin](https://docs.kucoin.com/#general)
    - [ ] [Binance](https://binance-docs.github.io/apidocs)
    - [ ] [BitPanda](https://developers.bitpanda.com/platform/)
- [ ] Defines a data model 
  - [X] for transactions
  - [X] for market data
  - [ ] for positions
  - [ ] for orders
- [ ] Crypto data
  - [ ] PnL
  - [ ] RSI Indicators
  - [ ] Moving Averages
- [ ] Defines events to be subscribed by bots
  - [ ] price change

# libraries

- python-dotenv
- [python-binance](https://python-binance.readthedocs.io/)
- kucoin-python
- coingecko-api-client

# Model

## Orders

**Time in force**
- GTC: Good til canceled
- IOC: Immediate Or Cancel
- FOK: Fill or Kill (an order will expire if the fill order cannot be filled upon execution) 
- GTT: Good Til Time

**Post only**

The post-only flag ensures that the trader always pays the maker fee and provides liquidity to the order book. If any part of the order is going to pay taker fee, the order will be fully rejected.

If a post only order will get executed immediately against the existing orders (except iceberg and hidden orders) in the market, the order will be cancelled.

For post only orders, it will get executed immediately against the iceberg orders and hidden orders in the market. Users placing the post only order will be charged the maker fees and the iceberg and hidden orders will be charged the taker fees.

**Self-Trade Prevention**

Self-Trade Prevention is an option in advanced settings.It is not selected by default. If you specify STP when placing orders, your order won't be matched by another one which is also yours. On the contrary, if STP is not specified in advanced, your order can be matched by another one of your own orders.

Market order is currently not supported for DC. When the timeInForce is set to FOK, the stp flag will be forcely specified as CN.

Market order is currently not supported for DC. When timeInForce is FOK, the stp flag will be forced to be specified as CN.

|Flag|	Name|
|-----|-----|
|DC|	Decrease and Cancel|
|CO|	Cancel oldest|
|CN|	Cancel newest|
|CB|	Cancel both|

### FTX Order

```yaml
avgFillPrice: 0.306526
clientId:
createdAt: '2019-03-05T09:56:55.728933+00:00'
filledSize: 10
future: XRP-PERP
id: 9596912
ioc: false
market: XRP-PERP
postOnly: false
price: 0.306525
reduceOnly: false
remainingSize: 31421
side: sell
size: 31431
status: open
type: limit
```

### Kucoin Order

```yaml
cancelAfter: 0
cancelExist: false
channel: IOS
clientOid: ''
createdAt: 1547026471000
dealFunds: '0.166'
dealSize: '2'
fee: '0'
feeCurrency: USDT
funds: '0'
hidden: false
iceberg: false
id: 5c35c02703aa673ceec2a168
isActive: false
opType: DEAL
postOnly: false
price: '10'
remark: ''
side: buy
size: '2'
stop: ''
stopPrice: '0'
stopTriggered: false
stp: ''
symbol: BTC-USDT
tags: ''
timeInForce: GTC
tradeType: TRADE
type: limit
visibleSize: '0'
```


### Binance Order

```yaml
clientOrderId: myOrder1
cummulativeQuoteQty: '0.0'
executedQty: '0.0'
icebergQty: '0.0'
isWorking: true
orderId: 1
orderListId: -1
origQty: '1.0'
origQuoteOrderQty: '0.000000'
price: '0.1'
side: BUY
status: NEW
stopPrice: '0.0'
symbol: LTCBTC
time: 1499827319559
timeInForce: GTC
type: LIMIT
updateTime: 1499827319559
```

## Fills

### FTX fills

```yaml
fee: 20.1374935
feeCurrency: USD
feeRate: 0.0005
future: EOS-0329
id: 11215
liquidity: taker
market: EOS-0329
baseCurrency:
quoteCurrency:
orderId: 8436981
tradeId: 1013912
price: 4.201
side: buy
size: 9587
time: '2019-03-27T19:15:10.204619+00:00'
type: order
```

### Kucoin fills

```yaml
counterOrderId: 5c1ab46003aa676e487fa8e3
createdAt: 1547026472000
fee: '0'
feeCurrency: USDT
feeRate: '0'
forceTaker: true
funds: '0.0699217232'
liquidity: taker
orderId: 5c35c02703aa673ceec2a168
price: '0.083'
side: buy
size: '0.8424304'
stop: ''
symbol: BTC-USDT
tradeId: 5c35c02709e4f67d5266954e
tradeType: TRADE
type: limit
```

### Binance Trade

```yaml
symbol: BNBBTC
id: 28457
orderId: 100234
orderListId: -1
price: '4.00000100'
qty: '12.00000000'
quoteQty: '48.000012'
commission: '10.10000000'
commissionAsset: BNB
time: 1499865549590
isBuyer: true
isMaker: false
isBestMatch: true
```