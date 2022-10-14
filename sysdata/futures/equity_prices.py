"""
Adjusted prices:

- back-adjustor
- just adjusted prices

"""

from sysdata.base_data import baseData
from sysobjects.equity_prices import equitySpotPrices

USE_CHILD_CLASS_ERROR = "You need to use a child class of equitiesSpotPricesData"


class equitySpotPricesData(baseData):
    """
    Read and write data class to get adjusted prices

    We'd inherit from this class for a specific implementation

    """

    def __repr__(self):
        return USE_CHILD_CLASS_ERROR

    def keys(self):
        return self.get_list_of_instruments()

    def get_equity_prices(self, instrument_code: str) -> equitySpotPrices:
        if self.is_code_in_data(instrument_code):
            spot_prices = self._get_equity_prices_without_checking(
                instrument_code
            )
        else:
            spot_prices = equitySpotPrices.create_empty()

        return spot_prices

    def __getitem__(self, instrument_code: str) -> equitySpotPrices:
        return self.get_equity_prices(instrument_code)

    def delete_equity_prices(self, instrument_code: str, are_you_sure: bool = False):
        if are_you_sure:
            if self.is_code_in_data(instrument_code):
                self._delete_equity_prices_without_any_warning_be_careful(
                    instrument_code
                )
                self.log.terse(
                    "Deleted equity price data for %s" % instrument_code,
                    instrument_code=instrument_code,
                )

            else:
                # doesn't exist anyway
                self.log.warn(
                    "Tried to delete non existent equity prices for %s"
                    % instrument_code,
                    instrument_code=instrument_code,
                )
        else:
            self.log.error(
                "You need to call delete_equity_prices with a flag to be sure",
                instrument_code=instrument_code,
            )

    def is_code_in_data(self, instrument_code: str) -> bool:
        if instrument_code in self.get_list_of_instruments():
            return True
        else:
            return False

    def add_equity_prices(
        self,
        instrument_code: str,
        equity_price_data: equitySpotPrices,
        ignore_duplication: bool = False,
    ):
        if self.is_code_in_data(instrument_code):
            if ignore_duplication:
                pass
            else:
                self.log.error(
                    "There is already %s in the data, you have to delete it first"
                    % instrument_code,
                    instrument_code=instrument_code,
                )

        self._add_equity_prices_without_checking_for_existing_entry(
            instrument_code, equity_price_data
        )

        self.log.terse(
            "Added data for instrument %s" % instrument_code,
            instrument_code=instrument_code,
        )

    def _add_equity_prices_without_checking_for_existing_entry(
        self, instrument_code: str, equity_price_data: equitySpotPrices
    ):
        raise NotImplementedError(USE_CHILD_CLASS_ERROR)

    def get_list_of_instruments(self) -> list:
        raise NotImplementedError(USE_CHILD_CLASS_ERROR)

    def _delete_equity_prices_without_any_warning_be_careful(
        self, instrument_code: str
    ):
        raise NotImplementedError(USE_CHILD_CLASS_ERROR)

    def _get_equity_prices_without_checking(
        self, instrument_code: str
    ) -> equitySpotPrices:
        raise NotImplementedError(USE_CHILD_CLASS_ERROR)
