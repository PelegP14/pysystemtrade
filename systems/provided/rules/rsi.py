def rsi(price, lookback):
    """
    Calculate the rsi trading rule forecast, given a price and lookback

    Assumes that 'price' is daily data

    This version does not do capping or scaling

    :param price: The price or other series to use (assumed Tx1)
    :type price: pd.Series

    :param lookback: Lookback for slow in days
    :type lookback: int

    :returns: pd.Series -- unscaled, uncapped forecast
    """
    delta = price.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(span=lookback).mean()
    ema_down = down.ewm(span=lookback).mean()
    rs = ema_up / ema_down
    rsi_val = 100 - (100 / (1 + rs))
    # make it so 0 is no position and negative is short and positive is long
    forecast = 50-rsi_val
    return forecast

def rsi_extremes(price,lookback):
    """
        Calculate the rsi trading rule forecast, given a price and lookback

        Assumes that 'price' is daily data

        This version does not do capping or scaling and only gives a signal when the indicator is at extremes

        :param price: The price or other series to use (assumed Tx1)
        :type price: pd.Series

        :param lookback: Lookback for slow in days
        :type lookback: int

        :returns: pd.Series -- unscaled, uncapped forecast
        """
    delta = price.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(span=lookback).mean()
    ema_down = down.ewm(span=lookback).mean()
    rs = ema_up / ema_down
    rsi_val = 100 - (100 / (1 + rs))
    overbought = rsi_val.clip(lower=70) - 70
    oversold = rsi_val.clip(upper=30) - 30
    forecast = overbought+oversold
    return forecast
