from sysdata.data_blob import dataBlob
from sysbrokers.IB.ib_equity_price_data import (
    ibEquityPriceData,
)
from sysbrokers.IB.ib_futures_contracts_data import ibFuturesContractData
from sysdata.arctic.arctic_equity_prices import (
    arcticEquitySpotPricesData,
)

from syscore.objects import failure, missing_data


def seed_equity_price_data_from_IB(instrument_code, ADD_TO_ARCTIC=True, ADD_TO_CSV=False):
    data = dataBlob()
    data.add_class_list(
        [
            ibEquityPriceData,
            arcticEquitySpotPricesData,
        ]
    )
    price_data = data.broker_equity_price.get_prices_at_frequency_for_equity(
        instrument_code
    )

    print(price_data)

    data.db_equity_spot_prices.add_equity_prices(
        instrument_code, price_data, ignore_duplication=True
    )

    return price_data

if __name__ == "__main__":
    print("Get initial price data from IB")
    instrument_code = input("Instrument code? <return to abort> ")
    if instrument_code == "":
        exit()

    seed_equity_price_data_from_IB(instrument_code)
