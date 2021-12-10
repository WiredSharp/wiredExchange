from enum import Enum


class CandleStickResolution(Enum):

    MIN_1   = '1min'
    MIN_3   = '3min'
    MIN_5   = '5min'
    MIN_15  = '15min'
    MIN_30  = '30min'
    HOUR_1  = '1hour'
    HOUR_2  = '2hour'
    HOUR_4  = '4hour'
    HOUR_6  = '6hour'
    HOUR_8  = '8hour'
    HOUR_12 = '12hour'
    DAY_1   = '1day'
    WEEK_1  = '1week'

    @staticmethod
    def from_seconds(resolution: int):
        resolved = _from_seconds[resolution]
        if resolved is None:
            raise KeyError(f'{resolution}: no candlestickResolution match')
        return resolved

    @staticmethod
    def to_seconds(resolution):
        resolved = _to_seconds[resolution]
        if resolved is None:
            raise KeyError(f'{resolution}: no resolution in second match')
        return resolved

_from_seconds = {
     60: CandleStickResolution.MIN_1,
    180: CandleStickResolution.MIN_3,
    300: CandleStickResolution.MIN_5,
    900: CandleStickResolution.MIN_15,
   1800: CandleStickResolution.MIN_30,
   3600: CandleStickResolution.HOUR_1,
   7200: CandleStickResolution.HOUR_2,
  14400: CandleStickResolution.HOUR_4,
  21600: CandleStickResolution.HOUR_6,
  28800: CandleStickResolution.HOUR_8,
  43200: CandleStickResolution.HOUR_12,
  86400: CandleStickResolution.DAY_1,
 604800: CandleStickResolution.WEEK_1
}


_to_seconds = {
     CandleStickResolution.MIN_1  : 60,
     CandleStickResolution.MIN_3  : 180,
     CandleStickResolution.MIN_5  : 300,
     CandleStickResolution.MIN_15 : 900,
     CandleStickResolution.MIN_30 : 1800,
     CandleStickResolution.HOUR_1 : 3600,
     CandleStickResolution.HOUR_2 : 7200,
     CandleStickResolution.HOUR_4 : 14400,
     CandleStickResolution.HOUR_6 : 21600,
     CandleStickResolution.HOUR_8 : 28800,
     CandleStickResolution.HOUR_12: 43200,
     CandleStickResolution.DAY_1  : 86400,
     CandleStickResolution.WEEK_1 : 604800
}