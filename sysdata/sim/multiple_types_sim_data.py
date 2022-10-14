from .sim_data import simData
import pandas as pd
import datetime

from syscore.objects import get_methods, missing_data
from syscore.dateutils import ARBITRARY_START
from syscore.pdutils import prices_to_daily_prices, get_intraday_df_at_frequency
from sysdata.base_data import baseData

from sysobjects.spot_fx_prices import fxPrices
from sysobjects.instruments import instrumentCosts, instrumentMetaData
from syslogdiag.log_to_screen import logtoscreen

class MultipleTypesSimData(simData):
    """
    class for holding data on multiple types of assets
    will override basic functions but for type specific functions,
    you will have to run over all the types this class holds
    """

    def __init__(self, data_list, log=logtoscreen("MultipleTypesSimData")):
        super().__init__(log)
        self.data_list = data_list

    def get_correct_data(self,instrument_code):
        for data in self.data_list:
            if instrument_code in data.get_instrument_list():
                return data
        raise KeyError("Dataset doesn't contain {}".format(instrument_code))

    def get_raw_price_from_start_date(
            self, instrument_code: str, start_date: datetime.datetime
    ) -> pd.Series:
        return self.get_correct_data(instrument_code).get_raw_price_from_start_date(instrument_code,start_date)

    def get_instrument_list(self) -> list:
        instrument_list = []
        for data in self.data_list:
            instrument_list += data.get_instrument_list()
        return instrument_list

    def get_value_of_block_price_move(self, instrument_code: str) -> float:
        return self.get_correct_data(instrument_code).get_value_of_block_price_move(instrument_code)

    def get_raw_cost_data(self, instrument_code: str) -> instrumentCosts:
        return self.get_correct_data(instrument_code).get_raw_cost_data(instrument_code)

    def get_instrument_currency(self, instrument_code: str) -> str:
        return self.get_correct_data(instrument_code).get_instrument_currency(instrument_code)

    def _get_fx_data_from_start_date(
            self, currency1: str, currency2: str, start_date: datetime.datetime
    ) -> fxPrices:
        for data in self.data_list:
            try:
                return data._get_fx_data_from_start_date(currency1,currency2,start_date)
            except:
                continue
        raise KeyError("couldn't get fx for pair {}{}".format(currency1,currency2))