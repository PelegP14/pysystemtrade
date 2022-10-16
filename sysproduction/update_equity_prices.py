"""
Update historical data per contract from interactive brokers data, dump into mongodb
"""

from copy import copy
from syscore.objects import success, failure, arg_not_supplied, missing_data
from syscore.merge_data import spike_in_data
from syscore.dateutils import DAILY_PRICE_FREQ, Frequency
from syscore.pdutils import merge_data_with_different_freq

from sysdata.data_blob import dataBlob
from sysdata.tools.manual_price_checker import manual_price_checker

from syslogdiag.email_via_db_interface import send_production_mail_msg

from sysobjects.equity_prices import equitySpotPrices

from sysproduction.data.prices import diagPrices, updatePrices
from sysproduction.data.broker import dataBroker
from sysdata.tools.cleaner import priceFilterConfig, get_config_for_price_filtering

NO_SPIKE_CHECKING = 99999999999.0

def update_equity_prices():
    """
    Do a daily update for futures contract prices, using IB historical data

    :return: Nothing
    """
    with dataBlob(log_name="Update-Equity-Prices") as data:
        update_historical_price_object = updateEquityPrices(data)
        update_historical_price_object.update_equity_prices()
    return success


class updateEquityPrices(object):
    def __init__(self, data):
        self.data = data

    def update_equity_prices(self):
        data = self.data
        update_equity_prices_with_data(data)


def update_equity_prices_with_data(data: dataBlob,
                                       interactive_mode: bool = False):
    cleaning_config = get_config_for_price_filtering(data)
    price_data = diagPrices(data)
    list_of_codes_all = price_data.get_list_of_instruments_in_equity_prices()
    for instrument_code in list_of_codes_all:
        data.log.label(instrument_code=instrument_code)
        update_historical_prices_for_instrument(instrument_code, data,
                                                cleaning_config = cleaning_config,
                                                interactive_mode = interactive_mode)


def update_historical_prices_for_instrument(instrument_code: str,
                                            data: dataBlob,
                                            cleaning_config: priceFilterConfig = arg_not_supplied,
                                            interactive_mode: bool = False):
    """
    Do a daily update for equity prices, using IB historical data

    :param instrument_code: str
    :param data: dataBlob
    :return: None
    """

    diag_prices = diagPrices(data)
    intraday_frequency = diag_prices.get_intraday_frequency_for_historical_download()
    daily_frequency = DAILY_PRICE_FREQ

    list_of_frequencies = [intraday_frequency, daily_frequency]

    for frequency in list_of_frequencies:
        get_and_add_prices_for_frequency(
            data,
            instrument_code,
            frequency=frequency,
            cleaning_config=cleaning_config,
            interactive_mode = interactive_mode
        )

    write_merged_prices_for_equity(data,
                                     instrument_code=instrument_code,
                                     list_of_frequencies=list_of_frequencies)

    return success


def get_and_add_prices_for_frequency(
    data: dataBlob,
    instrument_code: str,
    frequency: Frequency,
    cleaning_config: priceFilterConfig,
    interactive_mode: bool = False
):
    broker_data_source = dataBroker(data)

    broker_prices = broker_data_source.get_cleaned_prices_at_frequency_for_equity(
        instrument_code, frequency, cleaning_config = cleaning_config
    )

    if broker_prices is missing_data:
        print("Something went wrong with getting prices for %s to check" % instrument_code)
        return failure

    if len(broker_prices) == 0:
        print("No broker prices found for %s nothing to check" % instrument_code)
        return success


    if interactive_mode:
        print("\n\n Manually checking prices for %s \n\n" % instrument_code)
        max_price_spike = cleaning_config.max_price_spike

        price_data = diagPrices(data)
        old_prices = price_data.get_prices_at_frequency_for_equity(instrument_code,
                                                                            frequency=frequency)
        new_prices_checked = manual_price_checker(
            old_prices,
            broker_prices,
            type_new_data=equitySpotPrices,
            max_price_spike=max_price_spike
        )
        check_for_spike = False
    else:
        new_prices_checked = copy(broker_prices)
        check_for_spike = True

    error_or_rows_added = price_updating_or_errors(data = data,
                                                   frequency=frequency,
                                                   instrument_code=instrument_code,
                                                   new_prices_checked = new_prices_checked,
                                                   check_for_spike=check_for_spike,
                                                   cleaning_config=cleaning_config
                                                   )
    if error_or_rows_added is failure:
        return failure

    data.log.msg(
        "Added %d rows at frequency %s for %s"
        % (error_or_rows_added, frequency, instrument_code)
    )
    return success

def price_updating_or_errors(data: dataBlob,
                             frequency: Frequency,
                             instrument_code: str,
                             new_prices_checked: equitySpotPrices,
                             cleaning_config: priceFilterConfig,
                             check_for_spike: bool = True
                            ):

    price_updater = updatePrices(data)

    error_or_rows_added = price_updater.update_prices_at_frequency_for_equity(
        instrument_code=instrument_code,
        new_prices=new_prices_checked,
        frequency=frequency,
        check_for_spike=check_for_spike,
        max_price_spike = cleaning_config.max_price_spike
        )

    if error_or_rows_added is spike_in_data:
        report_price_spike(data, instrument_code)
        return failure

    if error_or_rows_added is failure:
        data.log.warn("Something went wrong when adding rows")
        return failure


    return error_or_rows_added

def report_price_spike(data: dataBlob, instrument_code: str):
    # SPIKE
    # Need to email user about this as will need manually checking
    msg = (
        "Spike found in prices for %s: need to manually check by running interactive_manual_check_historical_prices"
        % instrument_code
    )
    data.log.warn(msg)
    try:
        send_production_mail_msg(
            data, msg, "Price Spike %s" % instrument_code
        )
    except BaseException:
        data.log.warn(
            "Couldn't send email about price spike for %s" % instrument_code
        )

def write_merged_prices_for_equity(data: dataBlob,
                                     instrument_code: str,
                                     list_of_frequencies: list):

    ## note list of frequencies must have daily as last or groupby won't work with volume

    assert list_of_frequencies[-1]==DAILY_PRICE_FREQ

    diag_prices = diagPrices(data)
    price_updater = updatePrices(data)

    list_of_data = [diag_prices.get_prices_at_frequency_for_equity(instrument_code,
                                                                            frequency=frequency,
                                                                            )
                    for frequency in list_of_frequencies]

    merged_prices = merge_data_with_different_freq(list_of_data)

    price_updater.overwrite_merged_prices_for_equity(instrument_code=instrument_code,
                                                          new_prices=merged_prices)



if __name__ == '__main__':
    update_equity_prices()
