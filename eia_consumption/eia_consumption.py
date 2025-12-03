"""
Get EIA Consumption data.

This is the major python module that exposes EIA data.
"""


import os
import pandas as pd
import requests
import logging
import io
import json
from eia_api import read_eia_path
from eia_geography_mappings import (convert_native_name_to_standard_state_name,
                                                         get_fifty_us_states_and_dc,
                                                         get_united_states_name)
from datetime import datetime
import math
from scipy import integrate
import calendar
import matplotlib.pyplot as plt
from global_configurations import working_directory_location


def get_eia_consumption_data():


    df = get_eia_consumption_data_df(create_new_data=True,
                                     start_date="2022-01-01",
                                     end_date="2024-12-31")
    return df

def get_eia_consumption_file_name(state, eia_start_month, eia_end_month):

    return f"eia_monthly_consumption_{str(state)}_{str(eia_start_month)}_{str(eia_end_month)}.csv"

def get_eia_monthly_consumption(eia_start_month, eia_end_month, state="Virginia", consumption_type="Residential"):

    consumption_file_name = get_eia_consumption_file_name(state, eia_start_month, eia_end_month)
    if os.path.exists(consumption_file_name):
        df = pd.read_csv(consumption_file_name)
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        return df

    df = get_eia_consumption_data()
    df["Date"] = df["period"].apply(lambda dt: dt + "-01")
    df["Datetime"] = pd.to_datetime(df["Date"])
    df = df.query(f"standard_state_name == '{state}'")
    df_consumption = df[df["series-description"].apply(lambda s: consumption_type in s)] #TODO: This could be replaced with a filter.
    n, p = df.shape
    if n == 0:
        raise RuntimeError(f"No {state} Data is available.")

    df_consumption = df_consumption[df_consumption["Datetime"] >= datetime.strptime(eia_start_month, "%Y-%m-%d")]
    df_consumption = df_consumption[df_consumption["Datetime"] <= datetime.strptime(eia_end_month, "%Y-%m-%d")]
    df_consumption.to_csv(consumption_file_name)
    return df_consumption

def get_eia_mapping(canonical_component_name: str) -> dict:
    """
    Gets the mapping between the canonical component name and the EIA Component name.

    :param canonical_component_name:
    :return:
    """

    canonical_component_name_to_eia_name = dict()
    canonical_component_name_to_eia_name["Residential"] = "Residential Consumption"
    canonical_component_name_to_eia_name["Commercial"] = "Commercial Consumption"
    canonical_component_name_to_eia_name["Electric"] = "Electric Power Consumption"
    canonical_component_name_to_eia_name["Electric Power Consumption"] = "Electric Power Consumption"

    return canonical_component_name_to_eia_name


def get_api_test_path():
    """
    Gets the API call correctly for a test example.

    """

    return r"""https://api.eia.gov/v2/seriesid/ELEC.SALES.CO-RES.A?api_key=b8443fd367021d8fe4de53869989c0f2"""



def get_eia_consumption_path(start_date: str, end_date: str):
    """
    Get EIA Consumption data.

    It takes some searching to find the correct path to use in api path
    provided below.

    A few things to note:
        1. API Dashboard is useful to development of the correct path.
        2. Adding /data to the path is useful to get all data under a particular
        path.
        3. One needs to add the ?api_key=api_key=b8443fd367021d8fe4de53869989c0f2 at the
        end of the link.
        4. The useful api path is not in the API Dashboard but instead in the browser API in the
        search bar at the top.

    Useful links are provided by:
    -----------------------------
        1. https://www.eia.gov/opendata/documentation.php
        2. https://www.eia.gov/opendata/browser/

    In addition to query the relevant, values and not just what data might be there one needs to
    also add the &data[]=value, which is provided at the bottom of the documentation.

    We have some more information that is provided via the API:
    -----------------------------------------------------------
    -----------------------------------------------------------

    The response is a very large data set. We didn't specify any facets or filters,
    so the API returned as many values as it could. The API will not return more
    than 5,000 rows of data points. However, it will identify the total number of
    rows that are responsive to our request in the response header. In this case,
    7,440 data rows match the API request we just made.

    """

    return r"""https://api.eia.gov/v2/natural-gas/cons/sum/data?api_key=b8443fd367021d8fe4de53869989c0f2&data[]=value&start={0}&end={1}""".format(start_date, end_date)


def read_eia_consumption_data(start_date, end_date):

    eia_consumption_path = get_eia_consumption_path(start_date, end_date)
    result = read_eia_path(eia_consumption_path)
    return result

def test_read_api_path():

    eia_test_path = get_api_test_path()
    result = read_eia_path(eia_test_path)
    return result


def get_path_to_raw_eia():
    return "raw_eia_consumption_data.csv"

def get_eia_consumption_data_df(start_date = "2024-01-01",
                                end_date = "2024-10-01",
                                create_new_data=False):
    """
    Gets EIA Consumption data dataframe.

    The fields of the response are:
        1. 'warnings'
        2. 'total',
        3. 'dateFormat'
        4. 'frequency',
        5. 'data'

    """

    if os.path.exists(get_path_to_raw_eia()) and not create_new_data:
        return pd.read_csv(get_path_to_raw_eia())

    interval_range = pd.interval_range(start = datetime.strptime(start_date, "%Y-%m-%d"),
                                       end = datetime.strptime(end_date, "%Y-%m-%d"),
                                       freq = "MS",
                                       closed='left')

    dfs = []
    for interval in interval_range:

        start_date_str = str(interval.left)[:10]
        end_date_str = str(interval.right)[:10]

        api_call_successful, result = read_eia_consumption_data(start_date_str,
                                                                end_date_str)



        urlData = result.content
        urlDataDecoded = urlData.decode('utf-8')
        res = json.loads(urlDataDecoded)
        response = res.get('response')

        total = response.get('total')
        date_format = response.get('dateFormat')
        frequency = response.get('frequency')
        warnings = response.get('warnings')
        data = response.get('data')

        eia_consumption_df = pd.DataFrame.from_records(data)

        if not "period" in eia_consumption_df:
            raise RuntimeError(f"Period not found in the eia_consumption_df for the interval {interval}")

        assert(eia_consumption_df["period"].nunique() == 1)
        dfs.append(eia_consumption_df)

    eia_consumption_df = pd.concat(dfs)

    eia_consumption_df["standard_state_name"] = eia_consumption_df["area-name"].apply(
        lambda area_name: convert_native_name_to_standard_state_name(area_name))

    return eia_consumption_df


def query_eia_consumption_data(eia_consumption_df,
                               canonical_component_name: str):
    """
    Query the dataframe for the particular component.

    """

    eia_native_component_column_name = "process-name"
    eia_native_name = get_eia_mapping(canonical_component_name).get(canonical_component_name)
    eia_consumption_df_for_component_name = (eia_consumption_df[eia_consumption_df[
        eia_native_component_column_name].isin([eia_native_name])])

    return eia_consumption_df_for_component_name


def get_eia_consumption_data_in_pivot_format(start_date = "2000-01-01",
                                             end_date = "2024-10-01",
                                             canonical_component_name: str = "Residential",
                                             create_new_data=True):
    """
    Get all eia data from 2000-01-01 to 2024-01-01.

    The method calls EIA multiple times to get results.



    :return:
    """

    eia_consumption_df = get_eia_consumption_data_df(start_date = start_date,
                                                     end_date = end_date,
                                                     create_new_data=create_new_data)



    eia_consumption_df["Units"] = "MCCF"

    eia_consumption_for_component_df = query_eia_consumption_data(eia_consumption_df,
                                    canonical_component_name)

    eia_residential_df = eia_consumption_for_component_df.pivot(index='period',
                                                  columns='standard_state_name',
                                                  values='value')

    return eia_residential_df


def calculate_error_in_df(row: pd.Series):
    """
    Calculate error in the incoming EIA dataframe.


    :param row:
    :return:
    """


    united_states_name = get_united_states_name()
    state_names = get_fifty_us_states_and_dc()
    if united_states_name in row:
        united_states_value = row.get(united_states_name)
        if not type(united_states_value) == float:
            if (type(united_states_value) is str and united_states_value.isnumeric()) or united_states_value is None:
                if united_states_value is None:
                    united_states_value = float('nan')
                else:
                    united_states_value = float(united_states_value)
            else:
                raise ValueError("Value parsed from the dataframe is not. The value is provided "
                                 "by {}".format(united_states_value))
        else:
            pass
    else:
        raise RuntimeError(f"United States Value is not provided")

    united_states_value_aggregated_from_state_level = 0
    state_data_fully_provided = True
    for state_name in state_names:
        if state_name in row:
            state_value = row.get(state_name)
        else:
            raise RuntimeError(f"State provided by {state_name} is not provided")

        if state_value is None or state_value.isnumeric():
            if state_value is None:
                state_value = float('nan')
            else:
                state_value = float(state_value)
        else:
            raise ValueError(f"State value parsed from dataframe is a string not a number")

        if not math.isnan(state_value):
            united_states_value_aggregated_from_state_level += state_value
        else:
            state_data_fully_provided = False
            break

    if state_data_fully_provided:
        return united_states_value - united_states_value_aggregated_from_state_level
    else:
        return None


def calculate_state_aggregated_us_value_in_df(row: pd.Series):
    """
    Calculate state aggregated US value. This is equivalent to
    the summation of all 50 states that make up United States.

    """

    united_states_name = get_united_states_name()
    state_names = get_fifty_us_states_and_dc()
    if united_states_name in row:
        united_states_value = row.get(united_states_name)
        if not type(united_states_value) == float:
            if (type(united_states_value) is str and united_states_value.isnumeric()) or united_states_value is None:
                if united_states_value is None:
                    united_states_value = float('nan')
                else:
                    united_states_value = float(united_states_value)
            else:
                raise ValueError("Value parsed from the dataframe is not. The value is provided "
                                 "by {}".format(united_states_value))
        else:
            pass

    united_states_value_aggregated_from_state_level = 0
    state_data_fully_provided = True
    for state_name in state_names:
        if state_name in row:
            state_value = row.get(state_name)
        else:
            raise RuntimeError(f"State provided by {state_name} is not provided")

        if state_value is None or state_value.isnumeric():
            if state_value is None:
                state_value = float('nan')
            else:
                state_value = float(state_value)
        else:
            raise ValueError(f"State value parsed from dataframe is a string not a number")

        if not math.isnan(state_value):
            united_states_value_aggregated_from_state_level += state_value
        else:
            state_data_fully_provided = False
            break

    if state_data_fully_provided:
        return united_states_value_aggregated_from_state_level
    else:
        return None


def get_name_for_us_error():

    return "error_in_us_state_versus_us_state_aggregate"

def get_state_aggregate_column_name():
    return "us_value_state_aggregate_column_name"

def check_for_data_consistency(eia_consumption_df):
    """
    Runs checks for the EIA Residential pivot df to ensure consistency
    in the problem.

    The checks we would like to run are things like, does the summation of all
    the states add up to the US Aggregate number.

    """


    error_column_name = get_name_for_us_error()
    state_aggregate_column_name = get_state_aggregate_column_name()

    eia_consumption_df[error_column_name] = eia_consumption_df.apply(lambda row: calculate_error_in_df(row),
                                                                                                 axis=1)



    eia_consumption_df[state_aggregate_column_name] = eia_consumption_df.apply(lambda row: calculate_state_aggregated_us_value_in_df(row),
                                                                     axis=1)

    #Need to modify with the state aggregate number.
    #

    inconsistency = (eia_consumption_df[error_column_name].abs().sum() > 0)
    return eia_consumption_df, inconsistency


def get_number_of_mmcf_in_bcf():
    return 1000


def get_number_days_in_month(year: int, month: int):


    month_number, number_of_days_in_month = calendar.monthrange(year,month)
    return number_of_days_in_month

def get_number_of_days_in_month(date):
    return calendar.monthrange(date.year, date.month)[1]

def calculate_uniform_disaggregation(united_states_df):
    """
    Calculates the uniform disaggregation in time.

    :return:
    """

    united_states_df["Begin_Date"] = united_states_df["Date"]
    united_states_df["End_Date"] = united_states_df["Date"].shift(-1)

    def daily_uniform_disaggregation(date):

        united_states_filtered = united_states_df[united_states_df["Begin_Date"] <= date]
        united_states_filtered = united_states_filtered[date < united_states_df["End_Date"]]
        if len(united_states_filtered) == 1:
            return float(united_states_filtered["Value"].iloc[0]) / get_number_of_days_in_month(united_states_filtered["Date"].iloc[0])
        else:
            logging.info(f"Cannot calculate uniform disaggregation for the date {date}")
            return None

    return daily_uniform_disaggregation


def calculate_mean_and_std_for_daily_values_for_consumption(start_date: str,
                                                           end_date: str,
                                                           calculation_years = []):
    """
    Calculates EIA simulated daily values for Residential or Commercial Consumption for a
    particular period of time, beginning with the start_date and ending with the end date.

    Start Date = "2024-03-01"
    End Date = "2024-04-01"
    Calculation Years = [2022, 2023]

    If these are the arguments, then we can look at the (1) mean and (2) standard deviation
    of the values that are found during this period of time.

    The set of dates:
        (1) 2022-03-01 to 2022-04-01
        (2) 2023-03-01 to 2023-04-01

    The mean and standard deviation can be calculated from these set of dates.


    """

    pass