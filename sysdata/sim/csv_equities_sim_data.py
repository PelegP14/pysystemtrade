"""
Get data from .csv files used for futures trading

"""

from syscore.objects import arg_not_supplied
from sysdata.csv.csv_spot_fx import csvFxPricesData
from sysdata.csv.csv_instrument_data import csvInstrumentData
from sysdata.csv.csv_equity_prices import csvEquitySpotPricesData

from sysdata.data_blob import dataBlob
from sysdata.sim.equities_sim_data_with_data_blob import genericBlobUsingEquitiesSimData

from syslogdiag.log_to_screen import logtoscreen


class csvEquitiesSimData(genericBlobUsingEquitiesSimData):
    """
    Uses default paths for .csv files, pass in dict of csv_data_paths to modify
    """

    def __init__(
        self, csv_data_paths=arg_not_supplied, log=logtoscreen("csvEquitiesSimData")
    ):

        data = dataBlob(
            log=log,
            csv_data_paths=csv_data_paths,
            class_list=[
                csvEquitySpotPricesData,
                csvInstrumentData,
                csvFxPricesData,
            ],
        )

        super().__init__(data=data)

    def __repr__(self):
        return "csvEquitiesSimData object with %d instruments" % len(
            self.get_instrument_list()
        )
