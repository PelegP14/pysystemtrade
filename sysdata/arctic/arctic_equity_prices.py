from sysdata.futures.equity_prices import (
    equitySpotPricesData,
)
from sysobjects.equity_prices import equitySpotPrices
from sysdata.arctic.arctic_connection import arcticData
from syslogdiag.log_to_screen import logtoscreen
from syscore.dateutils import Frequency, DAILY_PRICE_FREQ, MIXED_FREQ
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

    def has_price_data_for_equity_at_frequency(self,
                                                 instrument_code: str,
                                                 frequency: Frequency) -> bool:

        return self.arctic.has_keyname(from_equity_and_freq_to_key(instrument_code,
                                                                                frequency=frequency))

    def get_equities_with_price_data_for_frequency(self,
                                                    frequency: Frequency) -> list:

        list_of_code_and_freq_tuples = self._get_equity_and_frequencies_with_price_data()
        list_of_contracts = [
            freq_and_code_tuple[1]
            for freq_and_code_tuple in list_of_code_and_freq_tuples
            if freq_and_code_tuple[0] == frequency
        ]

        return list_of_contracts

    def _get_equity_and_frequencies_with_price_data(self) -> list:
        """

        :return: list of futures contracts as tuples
        """

        all_keynames = self.arctic.get_keynames()
        list_of_contract_and_freq_tuples = [
            from_key_to_freq_and_code(keyname) for keyname in all_keynames
        ]

        return list_of_contract_and_freq_tuples

    def get_equities_with_merged_price_data(self) -> list:
        """

        :return: list of contracts
        """

        list_of_contracts = self.get_equities_with_price_data_for_frequency(frequency=MIXED_FREQ)

        return list_of_contracts

    def _get_merged_prices_for_equity_no_checking(
            self, instrument_code: str
    ) -> equitySpotPrices:
        """
        Read back the prices for a given contract object

        :param contract_object:  futuresContract
        :return: data
        """

        # Returns a data frame which should have the right format
        data = self._get_prices_at_frequency_for_equity_no_checking(instrument_code,
                                                                             frequency=MIXED_FREQ)

        return data

    def _write_merged_prices_for_equity_no_checking(
        self,
        instriment_code: str,
        equity_price_data: equitySpotPrices,
    ):
        """
        Write prices
        CHECK prices are overriden on second write

        :param futures_contract_object: futuresContract
        :param futures_price_data: futuresContractPriceData
        :return: None
        """

        self._write_prices_at_frequency_for_equity_no_checking(instrument_code=instriment_code,
                                                                        frequency=MIXED_FREQ,
                                                                        equity_price_data=equity_price_data)

    def _write_prices_at_frequency_for_equity_no_checking(
        self,
        instrument_code: str,
        equity_price_data: equitySpotPrices,
        frequency: Frequency
    ):

        ident = from_equity_and_freq_to_key(instrument_code,
                                              frequency=frequency)
        equity_price_data_as_pd = pd.DataFrame(equity_price_data)

        self.arctic.write(ident, equity_price_data_as_pd)

        self.log.msg(
            "Wrote %s lines of prices for %s at %s to %s"
            % (len(equity_price_data), str(instrument_code), str(frequency), str(self))
        )

def from_equity_and_freq_to_key(instrument_code: str,
                                  frequency: Frequency):
    if frequency is MIXED_FREQ:
        frequency_str = ""
    else:
        frequency_str = frequency.name+"/"

    return from_tuple_to_key([frequency_str, instrument_code])

def from_tuple_to_key(keytuple):
    return keytuple[0]+keytuple[1]

def from_key_to_freq_and_code(keyname):
    first_split = keyname.split("/")
    if len(first_split)==1:
        frequency = MIXED_FREQ
        instrument_code = keyname
    else:
        frequency = Frequency[first_split[0]]
        instrument_code = first_split[1]

    return frequency, instrument_code
