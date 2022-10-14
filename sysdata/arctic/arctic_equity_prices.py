from sysdata.futures.equity_prices import (
    equitySpotPricesData,
)
from sysobjects.equity_prices import equitySpotPrices
from sysdata.arctic.arctic_connection import arcticData
from syslogdiag.log_to_screen import logtoscreen
import pandas as pd

EQUITYPRICE_COLLECTION = "equity_spot_prices"


class arcticEquitySpotPricesData(equitySpotPricesData):
    """
    Class to read / write multiple futures price data to and from arctic
    """

    def __init__(self, mongo_db=None, log=logtoscreen("arcticEquitySpotPricesData")):

        super().__init__(log=log)

        self._arctic = arcticData(EQUITYPRICE_COLLECTION, mongo_db=mongo_db)

    def __repr__(self):
        return repr(self._arctic)

    @property
    def arctic(self):
        return self._arctic

    def get_list_of_instruments(self) -> list:
        return self.arctic.get_keynames()

    def _get_equity_prices_without_checking(
        self, instrument_code: str
    ) -> equitySpotPrices:
        data = self.arctic.read(instrument_code)

        instrpricedata = equitySpotPrices(data[data.columns[0]])

        return instrpricedata

    def _delete_equity_prices_without_any_warning_be_careful(
        self, instrument_code: str
    ):
        self.arctic.delete(instrument_code)
        self.log.msg(
            "Deleted adjusted prices for %s from %s" % (instrument_code, str(self)),
            instrument_code=instrument_code,
        )

    def _add_equity_prices_without_checking_for_existing_entry(
        self, instrument_code: str, equity_price_data: equitySpotPrices
    ):
        equity_price_data_aspd = pd.DataFrame(equity_price_data)
        equity_price_data_aspd.columns = ["price"]
        equity_price_data_aspd = equity_price_data_aspd.astype(float)
        self.arctic.write(instrument_code, equity_price_data_aspd)
        self.log.msg(
            "Wrote %s lines of prices for %s to %s"
            % (len(equity_price_data_aspd), instrument_code, str(self)),
            instrument_code=instrument_code,
        )

