from sysdata.sim.csv_futures_sim_data import csvFuturesSimData
from sysdata.sim.csv_equities_sim_data import csvEquitiesSimData
from sysdata.sim.multiple_types_sim_data import MultipleTypesSimData
from sysquant.estimators.vol import robust_vol_calc
from systems.provided.futures_chapter15.estimatedsystem import futures_system
from sysdata.config.configdata import Config
from syscore.dateutils import ROOT_BDAYS_INYEAR
import matplotlib.pyplot as plt
import os


path = os.path.abspath("backtest_estimated.yaml")
config = Config(path)
futures_data = csvFuturesSimData()
equities_data = csvEquitiesSimData()
print(equities_data['SPY'].index)
data = MultipleTypesSimData([equities_data,futures_data])
system = futures_system(config=config,data=data)
print(system.rawdata.rolls_per_year('EUR_micro'))
print(system.data.get_instrument_list())
system.set_logging_level("on")
# instruments = ['EUR_micro','SP500']
#
# for instrument_code in instruments:
#     price=data.daily_prices(instrument_code)
#     vol = robust_vol_calc(price.diff()/price)*ROOT_BDAYS_INYEAR
#     print(instrument_code,vol)

print(system.accounts.portfolio().percent.stats())
system.accounts.portfolio().percent.curve().plot()
plt.show()
print(system.accounts.portfolio().stats())
system.accounts.portfolio().curve().plot()
plt.show()


# data=csvFuturesSimData()
# for instrument_code in data.get_instrument_list():
#     price=data.daily_prices(instrument_code)
#     factor = data.get_value_of_block_price_move(instrument_code)/10
#     fx_exchange = data.get_fx_for_instrument(instrument_code,"USD")
#     vol = robust_vol_calc(price.diff()/price)
#     target_risk = 0.12
#     min_capital = price[-1]*factor*fx_exchange[-1]*16*vol[-1]/target_risk
#     if min_capital < 16000:
#         print("instrument ok to trade {} min capital {} max leverage {}".format(instrument_code,min_capital,2*target_risk/(16*vol[-1])))



