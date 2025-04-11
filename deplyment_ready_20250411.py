import requests
import datetime
import pandas as pd
import os
import json
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from datetime import date, datetime, timedelta
import pandas_gbq
from google.oauth2 import service_account
import copy
from datetime import datetime
# from datetime import datetime
from dateutil.relativedelta import relativedelta, SU


# Reading BQ table
def get(var, project_id):
    """
    Purpose:
        Get dataframe from BQ based on sql query
    Arguments:
        var : SQL query
    Returns:
        df [dataframe]: Dataframe returned from BQ
    """
    try:
        df = pandas_gbq.read_gbq(var, project_id=project_id)
        print('Reading Successful')
    except Exception as e:
        print('Reading data NOT successful ' + str(e) + str(e.__traceback__.tb_lineno))
        exit(1)
    return df


def set_option():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', None)


set_option()


def get_sales_data(start_date):
    # read walmart ids with sales data
    walmart_ids1 = pd.read_excel("52week sales data in vertical format - Copy.xlsx")
    df = walmart_ids1.copy()

    # fill the missing weeks with na values
    df['period_week'] = pd.to_datetime(df['period_week'])

    # Group and reindex within each walmart_id's own date range
    def reindex_group(group):
        full_range = pd.date_range(start=group['period_week'].min(),
                                   end=group['period_week'].max(),
                                   freq='W-SUN')
        return group.set_index('period_week').reindex(full_range).assign(
            walmart_id=group['walmart_id'].iloc[0]).reset_index().rename(columns={'index': 'period_week'})

    # Apply per group for each walmart id
    filled_df = df.groupby('walmart_id', group_keys=False).apply(reindex_group)

    filled_df = filled_df[['walmart_id', 'period_week', 'sales']]

    # fill the missing nan values using interpolate method
    filled_df['sales'] = filled_df['sales'].interpolate(method='linear')

    df = filled_df.copy()
    df_temp = filled_df.copy()
    # forecasting for each Wamart ID
    today = datetime.now()
    # Set 'period_week' as the index for time series forecasting
    df.set_index('period_week', inplace=True)

    # Function to forecast sales using a 26-week moving average for each Walmart ID
    def forecast_sales_for_walmart(walmart_data, forecast_weeks=12, window=26):
        # List to hold forecasted sales
        forecasted_sales = []

        # Previous sales data for the Walmart ID
        previous_sales = walmart_data['sales'].tolist()

        # Forecast for the next 12 weeks (3 months) based on 26-week moving average
        for week in range(forecast_weeks):
            # Ensure we have at least 'window' weeks of data to compute the moving average
            if len(previous_sales) >= window:
                moving_average = np.mean(previous_sales[-window:])
            else:
                moving_average = np.mean(previous_sales)  # If we don't have enough data

            # Append the forecasted sales for this week
            forecasted_sales.append(moving_average)

            # Add the forecasted sales to the list of previous sales (to calculate the next moving average)
            previous_sales.append(moving_average)

        # Generate future dates for the forecast
        forecast_dates = pd.date_range(walmart_data.index[-1] + pd.Timedelta(weeks=1), periods=forecast_weeks, freq='W')

        # Create a DataFrame for the forecasted data
        forecast_data = pd.DataFrame({
            'walmart_id': [walmart_data['walmart_id'].iloc[-1]] * forecast_weeks,  # Same Walmart ID
            'period_week': forecast_dates,
            'sales': forecasted_sales,
            '26_week_MA': forecasted_sales,  # Forecasted sales and 26-week MA will be the same initially
        })

        return forecast_data

    # # Group by Walmart ID and forecast for each group
    forecasted_data = []

    for walmart_id, walmart_data in df.groupby('walmart_id'):
        # Get the last available date for this Walmart ID
        print("walmart_id", walmart_id)
        last_date_in_data = walmart_data.index.max()
        print("last_date_in_data", last_date_in_data)
        # Calculate how many weeks to forecast from that point to today
        remaining_weeks = pd.date_range(last_date_in_data, today, freq='W').size
        print("remaining_weeks", remaining_weeks)
        # # Adjust forecast_weeks to be at least 12 weeks (or the remaining weeks, whichever is greater)
        # Use max of 12 or remaining_weeks
        forecast_weeks = max(12, remaining_weeks)
        print("forecast_weeks", forecast_weeks)
        forecast_df = forecast_sales_for_walmart(walmart_data, forecast_weeks=forecast_weeks)
        forecasted_data.append(forecast_df)

    #         forecasted values appended and sorted by walmart_id and period in asc order
    #     df_combined = pd.concat(forecasted_data)
    df_combined = pd.concat([df_temp] + forecasted_data)
    df_combined1 = df_combined.sort_values(by=['walmart_id', 'period_week']).reset_index(drop=True)

    #     if no of weeks available is less than 26 then flag it to 1
    weeks_ava = walmart_ids1.groupby("walmart_id")["period_week"].count().reset_index(name='weeks_available')
    weeks_ava['flag'] = weeks_ava['weeks_available'].apply(lambda x: 1 if x < 26 else 0)

    # get anchor value for current week
    anchor_df = df_combined1[df_combined1['period_week'] == start_date]
    return anchor_df, weeks_ava


def get_walmart_buybox_data(start_date):
    bb_data = pd.read_csv("loreal buybox data_50.csv")
    bb_data['period'] = pd.to_datetime(bb_data['period'])
    bb_data1=bb_data[bb_data['period']== start_date]
    print("bb_data1.shape",bb_data1.shape)
    return bb_data1


def main(start_date):
    # get buy box data
    bb_df = get_walmart_buybox_data(start_date)
    # get sales data
    sales_df, weeksava_df = get_sales_data(start_date)
    merging = pd.merge(bb_df, sales_df[['walmart_id', 'sales', 'period_week']], on=['walmart_id'], how='left')

    merging.rename(columns={'sales': 'anchor'}, inplace=True)
    # ----handle the case when sales data not present for any walmart isd then take avg of anchor value of
    # ---all avaialble walmart id for that period and substitute by that value
    # Calculate the mean anchor per walmart_id for the period 2025-03-30
    mean_anchor = merging[merging['period'] == start_date].groupby('walmart_id')['anchor'].mean()

    # Replace NaN values in the 'anchor' column with the mean anchor for that walmart_id
    merging['anchor'] = merging.apply(
        lambda row: mean_anchor[row['walmart_id']] if pd.isna(row['anchor']) else row['anchor'], axis=1)
    # calculate lost sales
    merging['lost_sales'] = np.round(merging['anchor'] * (merging['buy_box_percent'] / 100), 2)

    # add flag column indicating IDs having less than 26 week sales data
    final = pd.merge(merging, weeksava_df, on='walmart_id', how='left')
    final1 = final[
        ['walmart_id', 'seller_name', 'seller_id', 'buy_box_price', 'buy_box_percent', 'bb_win_count', 'period',
         'last_won_bb_date', 'anchor', 'lost_sales', 'flag']]
    final1.columns = ['walmart_id', 'seller_name', 'seller_id', 'buy_box_price',
                      'buy_box_percent', 'bb_win_count', 'period',
                      'last_won_bb_date', 'anchor', 'lost_sales', 'flag']
    result_order = ['walmart_id', 'seller_name', 'seller_id', 'buy_box_price',
                    'buy_box_percent', 'last_won_bb_date', 'bb_win_count', 'period',
                    'anchor', 'lost_sales', 'flag']
    result = final1[result_order]
    result.to_excel("lost sales walmart loreal _2025-04-11.xlsx", index=False)

if __name__ == "__main__":
    start_date = '2025-03-30'
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Current Time =", current_time)
    main(start_date)
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Current Time =", current_time)
