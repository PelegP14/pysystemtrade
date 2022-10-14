from sysdata.sim.equities_sim_data import equitiesSimData

from sysdata.futures.equity_prices import equitySpotPricesData
from sysdata.fx.spotfx import fxPricesData
from sysdata.futures.instruments import InstrumentData
from sysdata.data_blob import dataBlob


from sysobjects.instruments import (
    assetClassesAndInstruments,
    InstrumentWithMetaData,
)
from sysobjects.spot_fx_prices import fxPrices
from sysobjects.equity_prices import equitySpotPrices


class genericBlobUsingEquitiesSimData(equitiesSimData):
    """
    dataBlob must have the appropriate classes added or it won't work
    """

    def __init__(self, data: dataBlob):
        super().__init__(log=data.log)
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def db_fx_prices_data(self) -> fxPricesData:
        return self.data.db_fx_prices

    @property
    def db_equity_spot_prices_data(self) -> equitySpotPricesData:
        return self.data.db_equity_spot_prices

    @property
    def db_instrument_data(self) -> InstrumentData:
        return self.data.db_instrument

    def get_instrument_list(self):
        return self.db_equity_spot_prices_data.get_list_of_instruments()

    def _get_fx_data_from_start_date(
        self, currency1: str, currency2: str, start_date
    ) -> fxPrices:
        fx_code = currency1 + currency2
        data = self.db_fx_prices_data.get_fx_prices(fx_code)

        data_after_start = data[start_date:]

        return data_after_start

    def get_instrument_asset_classes(self) -> assetClassesAndInstruments:
        all_instrument_data = self.get_all_instrument_data_as_df()
        asset_classes = all_instrument_data["AssetClass"]
        asset_class_data = assetClassesAndInstruments.from_pd_series(asset_classes)

        return asset_class_data

    def get_all_instrument_data_as_df(self):
        all_instrument_data = (
            self.db_instrument_data.get_all_instrument_data_as_df()
        )
        instrument_list= self.get_instrument_list()
        all_instrument_data = all_instrument_data[all_instrument_data.index.isin(instrument_list)]

        return all_instrument_data

    def get_equities_spot_price(
        self, instrument_code: str
    ) -> equitySpotPrices:
        data = self.db_equity_spot_prices_data.get_equity_prices(instrument_code)

        return data

    def get_instrument_meta_data(
        self, instrument_code: str
    ) -> InstrumentWithMetaData:
        ## cost and other meta data stored in the same place
        return self.get_instrument_object_with_meta_data(instrument_code)

    def get_instrument_object_with_meta_data(
        self, instrument_code: str
    ) -> InstrumentWithMetaData:
        instrument = self.db_instrument_data.get_instrument_data(
            instrument_code
        )

        return instrument
