import datetime
import pandas as pd
from syscore.merge_data import merge_newer_data, full_merge_of_existing_data, spike_in_data

VERY_BIG_NUMBER = 999999.0

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

    def remove_zero_prices(self):
        drop_it = self == 0.0
        new_data = self[~drop_it]
        return equitySpotPrices(new_data)

    def remove_negative_prices(self):
        drop_it = self < 0.0
        new_data = self[~drop_it]
        return equitySpotPrices(new_data)


    def remove_future_data(self):
        new_data = equitySpotPrices(self[self.index < datetime.datetime.now()])

        return new_data

    def add_rows_to_existing_data(
        self, new_equity_prices, check_for_spike=True,
            max_price_spike: float = VERY_BIG_NUMBER
    ):
        """
        Merges self with new data.
        Only newer data will be added

        :param new_futures_per_contract_prices: another futures per contract prices object

        :return: merged futures_per_contract object
        """

        merged_futures_prices = merge_newer_data(
            pd.DataFrame(self),
            new_equity_prices,
            check_for_spike=check_for_spike,
            max_spike = max_price_spike,
        )

        if merged_futures_prices is spike_in_data:
            return spike_in_data

        merged_futures_prices = equitySpotPrices(merged_futures_prices)

        return merged_futures_prices