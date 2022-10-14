from copy import copy

import numpy as np
import pandas as pd

from syscore.objects import named_object
from syscore.merge_data import full_merge_of_existing_series
from sysobjects.dict_of_named_futures_per_contract_prices import (
    price_column_names,
    contract_column_names,
    price_name,
    contract_name_from_column_name,
)
from sysobjects.multiple_prices import futuresMultiplePrices


class equitySpotPrices(pd.Series):
    """
    spot price information
    """

    def __init__(self, price_data):
        price_data.index.name = "index"  # arctic compatible
        super().__init__(price_data)

    @classmethod
    def create_empty(equitySpotPrices):
        """
        Our graceful fail is to return an empty, but valid, dataframe
        """

        equity_spot_prices = equitySpotPrices(pd.Series())

        return equity_spot_prices