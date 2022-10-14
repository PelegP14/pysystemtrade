import pandas as pd

from sysdata.futures.equity_prices import equitySpotPricesData
from sysobjects.equity_prices import equitySpotPrices
from syscore.fileutils import get_filename_for_package, files_with_extension_in_pathname
from syscore.pdutils import pd_readcsv
from syscore.objects import arg_not_supplied
from syslogdiag.log_to_screen import logtoscreen

EQUITY_PRICES_DIRECTORY = "data.equities.spot_prices_csv"
DATE_INDEX_NAME = "DATETIME"


class csvEquitySpotPricesData(equitySpotPricesData):
    """

    Class for equity prices write / to from csv
    """

    def __init__(
        self, datapath=arg_not_supplied, log=logtoscreen("csvEquitySpotPriceData")
    ):

        super().__init__(log=log)

        if datapath is arg_not_supplied:
            datapath = EQUITY_PRICES_DIRECTORY

        self._datapath = datapath

    def __repr__(self):
        return "csvEquitySpotPriceData accessing %s" % self._datapath

    @property
    def datapath(self):
        return self._datapath

    def get_list_of_instruments(self) -> list:
        return files_with_extension_in_pathname(self.datapath, ".csv")

    def _get_equity_prices_without_checking(
        self, instrument_code: str
    ) -> equitySpotPrices:
        filename = self._filename_given_instrument_code(instrument_code)

        try:
            instrpricedata = pd_readcsv(filename)
        except OSError:
            self.log.warn("Can't find equity price file %s" % filename)
            return equitySpotPrices.create_empty()

        instrpricedata.columns = ["price"]
        instrpricedata = instrpricedata.groupby(level=0).last()
        instrpricedata = pd.Series(instrpricedata.iloc[:, 0])

        instrpricedata = equitySpotPrices(instrpricedata)

        return instrpricedata

    def _delete_equity_prices_without_any_warning_be_careful(
        self, instrument_code: str
    ):
        raise NotImplementedError(
            "You can't delete equity prices stored as a csv - Add to overwrite existing or delete file manually"
        )

    def _add_equity_prices_without_checking_for_existing_entry(
        self, instrument_code: str, equity_price_data: equitySpotPrices
    ):

        # Ensures the file will be written with a column header
        equity_price_data_as_dataframe = pd.DataFrame(equity_price_data)
        equity_price_data_as_dataframe.columns = ["price"]

        filename = self._filename_given_instrument_code(instrument_code)
        equity_price_data_as_dataframe.to_csv(filename, index_label=DATE_INDEX_NAME)

    def _filename_given_instrument_code(self, instrument_code: str):
        return get_filename_for_package(self.datapath, "%s.csv" % (instrument_code))
