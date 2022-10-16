from syscore.dateutils import Frequency, DAILY_PRICE_FREQ

from syscore.objects import missing_contract, missing_data


from sysdata.futures.equity_prices import (
    equitySpotPricesData,
)

from sysobjects.equity_prices import equitySpotPrices

from sysexecution.tick_data import tickerObject, dataFrameOfRecentTicks
from sysexecution.orders.contract_orders import contractOrder
from sysexecution.orders.broker_orders import brokerOrder

from sysobjects.futures_per_contract_prices import futuresContractPrices
from sysobjects.contracts import futuresContract, listOfFuturesContracts

from syslogdiag.log_to_screen import logtoscreen


class brokerEquityPriceData(equitySpotPricesData):
    """
    Class to read equity price data from a broker
    """

    def __init__(self, log=logtoscreen("brokerEquityPriceData")):

        super().__init__(log=log)

    def get_list_of_instruments(self) -> list:
        raise NotImplementedError

    def _get_equity_prices_without_checking(
        self, instrument_code: str
    ) -> equitySpotPrices:
        raise NotImplementedError

    def _delete_equity_prices_without_any_warning_be_careful(
        self, instrument_code: str
    ):
        raise NotImplementedError("Broker is a read only source of prices")

    def _add_equity_prices_without_checking_for_existing_entry(
        self, instrument_code: str, equity_price_data: equitySpotPrices
    ):
        raise NotImplementedError("Broker is a read only source of prices")

    def _write_merged_prices_for_equity_no_checking(self, *args, **kwargs):
        raise NotImplementedError("Broker is a read only source of prices")

