"""
Adjusted prices:

- back-adjustor
- just adjusted prices

"""

from sysdata.base_data import baseData
from sysobjects.equity_prices import equitySpotPrices
from syscore.dateutils import Frequency, DAILY_PRICE_FREQ, MIXED_FREQ
from syscore.objects import missing_data, failure
from syscore.merge_data import spike_in_data


USE_CHILD_CLASS_ERROR = "You need to use a child class of equitiesSpotPricesData"

VERY_BIG_NUMBER = 999999.0


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

    def get_prices_at_frequency_for_equity(self, instrument_code: str, frequency: Frequency = DAILY_PRICE_FREQ,
                                                    return_empty: bool = True) -> equitySpotPrices:
        """
        get some prices at a given frequency

        :param contract_object:  futuresContract
        :param frequency: str; one of D, H, 5M, M, 10S, S
        :return: data
        """

        if self.has_price_data_for_equity_at_frequency(instrument_code, frequency=frequency):
            return self._get_prices_at_frequency_for_equity_no_checking(instrument_code, frequency=frequency)
        else:
            if return_empty:
                return equitySpotPrices.create_empty()
            else:
                return missing_data

    def has_price_data_for_equity_at_frequency(self,
                                                 instrument_code: str,
                                                 frequency: Frequency) -> bool:

        list_of_contracts = \
            self.get_equities_with_price_data_for_frequency(frequency=frequency)
        if instrument_code in list_of_contracts:
            return True
        else:
            return False

    def get_equities_with_price_data_for_frequency(self,
                                                    frequency: Frequency) -> list:

        raise NotImplementedError(USE_CHILD_CLASS_ERROR)

    def _get_prices_at_frequency_for_equity_no_checking \
                    (self, instrument_code: str, frequency: Frequency) -> equitySpotPrices:

        raise NotImplementedError(USE_CHILD_CLASS_ERROR)

    def update_prices_at_frequency_for_equity(
            self,
            instrument_code: str,
            new_equity_spot_prices: equitySpotPrices,
            frequency: Frequency,
            check_for_spike: bool = True,
            max_price_spike: float = VERY_BIG_NUMBER
    ) -> int:

        if len(new_equity_spot_prices) == 0:
            self.log.msg("No new data")
            return 0

        if frequency is MIXED_FREQ:
            old_prices = self.get_merged_prices_for_equity(instrument_code)
        else:
            old_prices = self.get_prices_at_frequency_for_equity(instrument_code, frequency=frequency)

        merged_prices = old_prices.add_rows_to_existing_data(
            new_equity_spot_prices,
            check_for_spike=check_for_spike,
            max_price_spike=max_price_spike
        )

        if merged_prices is spike_in_data:
            self.log.msg(
                "Price has moved too much - will need to manually check - no price update done"
            )
            return spike_in_data

        rows_added = len(merged_prices) - len(old_prices)

        if rows_added < 0:
            self.log.critical("Can't remove prices something gone wrong!")
            return failure

        elif rows_added == 0:
            if len(old_prices) == 0:
                self.log.msg("No existing or additional data")
                return 0
            else:
                self.log.msg("No additional data since %s " % str(old_prices.index[-1]))
            return 0

        # We have guaranteed no duplication
        if frequency is MIXED_FREQ:
            self.write_merged_prices_for_equity(
                instrument_code, merged_prices, ignore_duplication=True
            )
        else:
            self.write_prices_at_frequency_for_equity(
                instrument_code, merged_prices, frequency=frequency,
                ignore_duplication=True
            )

        self.log.msg("Added %d additional rows of data" % rows_added)

        return rows_added

    def get_merged_prices_for_equity(self,
                                              instrument_code: str,
                                              return_empty: bool = True
                                              ) -> equitySpotPrices:
        """
        get all prices without worrying about frequency

        :param contract_object:  futuresContract
        :return: data
        """

        if self.has_merged_price_data_for_equity(instrument_code):
            prices = self._get_merged_prices_for_equity_no_checking(instrument_code)
        else:
            if return_empty:
                return equitySpotPrices.create_empty()
            else:
                return missing_data

        return prices

    def has_merged_price_data_for_equity(self, instrument_code: str) -> bool:
        list_of_contracts = self.get_equities_with_merged_price_data()
        if instrument_code in list_of_contracts:
            return True
        else:
            return False

    def write_merged_prices_for_equity(
        self,
        instrument_code: str,
        equity_price_data: equitySpotPrices,
        ignore_duplication=False,
    ):
        """
        Write some prices

        :param futures_contract_object:
        :param futures_price_data:
        :param ignore_duplication: bool, to stop us overwriting existing prices
        :return: None
        """
        not_ignoring_duplication = not ignore_duplication
        if not_ignoring_duplication:
            if self.has_merged_price_data_for_equity(instrument_code):
                self.log.warn(
                    "There is already existing data for %s"
                    % instrument_code
                )
                return None

        self._write_merged_prices_for_equity_no_checking(
            instrument_code, equity_price_data
        )

    def write_prices_at_frequency_for_equity(
        self,
        instrument_code: str,
        equity_price_data: equitySpotPrices,
        frequency: Frequency,
        ignore_duplication=False,
    ):
        """
        Write some prices

        :param futures_contract_object:
        :param futures_price_data:
        :param ignore_duplication: bool, to stop us overwriting existing prices
        :return: None
        """
        not_ignoring_duplication = not ignore_duplication
        if not_ignoring_duplication:
            if self.has_price_data_for_equity_at_frequency(instrument_code=instrument_code,
                                                             frequency=frequency):
                self.log.warn(
                    "There is already existing data for %s"
                    % instrument_code
                )
                return None

        self._write_prices_at_frequency_for_equity_no_checking(
            instrument_code=instrument_code,
            equity_price_data=equity_price_data,
            frequency=frequency
        )

    def get_equities_with_merged_price_data(self) -> list:

        raise NotImplementedError(USE_CHILD_CLASS_ERROR)

    def _get_merged_prices_for_equity_no_checking(
        self, insrument_code: str
    ) -> equitySpotPrices:

        raise NotImplementedError(USE_CHILD_CLASS_ERROR)

    def _write_merged_prices_for_equity_no_checking(
        self,
        instrument_code: str,
        equity_price_data: equitySpotPrices,
    ):

        raise NotImplementedError(USE_CHILD_CLASS_ERROR)

    def _write_prices_at_frequency_for_equity_no_checking(
        self,
        instrument_code: str,
        equity_price_data: equitySpotPrices,
        frequency: Frequency,
    ):

        raise NotImplementedError(USE_CHILD_CLASS_ERROR)