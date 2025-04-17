import functions_framework
import requests
import datetime
import pandas as pd
import os
import json
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, PermissionDenied, Conflict
from datetime import date, datetime, timedelta
import pandas_gbq
from google.oauth2 import service_account
import copy
from datetime import datetime
# from datetime import datetime
from dateutil.relativedelta import relativedelta, SU
import logging
import resource
import time

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

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


def write_output_to_bq(df, project_id):
    """
    write output dataframe to BQ tables for specific projectid mentioned
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.CC_I2O_DATA_MART.viz_lost_sales_walmart"

    # Define the schema
    schema = [
        bigquery.SchemaField("walmart_id", "STRING"),
        bigquery.SchemaField("seller_name", "STRING"),
        bigquery.SchemaField("buy_box_percent", "FLOAT"),
        bigquery.SchemaField("period", "STRING"),
        bigquery.SchemaField("anchor", "FLOAT"),
        bigquery.SchemaField("lost_sales", "FLOAT"),
        bigquery.SchemaField("flag", "FLOAT"),
        bigquery.SchemaField("report_run_date", "STRING")

    ]

    # Check if the table exists; if not, create it
    try:
        client.get_table(table_id)
        print(f"Table {table_id} exists.")
    except NotFound:
        # Create the table if it does not exist
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"Table {table_id} created.")

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema
    )

    period = df['period'].unique().tolist()
    period_formatted = ', '.join([f"'{p}'" for p in period])
    print('period_formatted_formatted', period_formatted)

    walmart_id = df['walmart_id'].unique().tolist()
    walmart_id_formatted = ', '.join([f"'{ot}'" for ot in walmart_id])

    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE 

        period IN ({period_formatted}) AND walmart_id IN ({walmart_id_formatted})

    """
    query_job = client.query(delete_query)
    query_job.result()  # Wait for the job to complete
    print(f"Deleted existing records for report_run_date: {period_formatted} from {table_id}.")


    # Load new data into the mart table
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    print(f"Loaded {len(df)} rows into {table_id}.")


def set_option():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', None)


set_option()


def get_sales_data(project_id, start_date):
    """
    Retrieves, processes, and forecasts Walmart sales data.
    
    This function performs multiple operations:
    1. Queries sales data from the CC_I2O_DATA_MART database
    2. Processes time series data by filling missing values using linear interpolation
    3. Generates sales forecasts using a 26-week moving average method
    4. Flags products with insufficient historical data (less than 26 weeks)
    
    Parameters:
    ----------
    project_id : str
        The ID of the project used for database connection/authentication.
    start_date : datetime or str
        The specific date for which to extract "anchor" sales values.
        
    Returns:
    -------
    tuple
        A tuple containing two pandas DataFrames:
        1. anchor_df: DataFrame with sales data for the specified start_date, containing:
           - walmart_id: Product identifier
           - period: The date of the data (matching start_date)
           - sales: Sales value for that product and date
        2. weeks_ava: DataFrame with information about data availability, containing:
           - walmart_id: Product identifier
           - weeks_available: Count of weeks with data for each product
           - flag: Binary indicator (1 if weeks_available < 26, 0 otherwise)
    """
    logger.info("Fetching sales data...")

    try:
        # read walmart ids with sales data
        sales_query = "Select * from CC_I2O_DATA_MART.viz_walmart_input_sales"
        sales_data = get(sales_query, project_id)
        sales_data['week'] = pd.to_datetime(sales_data['week'], format='%Y-%m-%d')
        sales_data['period'] = pd.to_datetime(sales_data['period'], format='%Y-%m-%d')
        sales_data.info()
        max_date = sales_data['week'].max()
        sales_data1 = sales_data[sales_data['week'] == max_date]
        df = sales_data1.copy()

        # Group and reindex within each walmart_id's own date range
        def reindex_group(group):
            full_range = pd.date_range(start=group['period'].min(),
                                    end=group['period'].max(),
                                    freq='W-SUN')
            return group.set_index('period').reindex(full_range).assign(
                walmart_id=group['walmart_id'].iloc[0]).reset_index().rename(columns={'index': 'period'})

        # Apply per group for each walmart id
        filled_df = df.groupby('walmart_id', group_keys=False).apply(reindex_group)

        filled_df = filled_df[['walmart_id', 'period', 'sales']]
        filled_df['sales'] = filled_df['sales'].astype(float)
        # fill the missing nan values using interpolate method
        filled_df['sales'] = filled_df['sales'].interpolate(method='linear')

        df = filled_df.copy()
        df_temp = filled_df.copy()
        # forecasting for each Wamart ID
        today = datetime.now()
        # Set 'period_week' as the index for time series forecasting
        df.set_index('period', inplace=True)

        # Function to forecast sales using a 26-week moving average for each Walmart ID
        def forecast_sales_for_walmart(walmart_data, forecast_weeks=12, window=26):
            """Generates sales forecasts for a specific Walmart product using a moving average approach."""
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
                'period': forecast_dates,
                'sales': forecasted_sales,
                '26_week_MA': forecasted_sales,  # Forecasted sales and 26-week MA will be the same initially
            })

            return forecast_data

        # # Group by Walmart ID and forecast for each group
        forecasted_data = []

        for walmart_id, walmart_data in df.groupby('walmart_id'):
            # Get the last available date for this Walmart ID
            # print("walmart_id", walmart_id)
            last_date_in_data = walmart_data.index.max()
            # print("last_date_in_data", last_date_in_data)
            # Calculate how many weeks to forecast from that point to today
            remaining_weeks = pd.date_range(last_date_in_data, today, freq='W').size
            # print("remaining_weeks", remaining_weeks)
            # # Adjust forecast_weeks to be at least 12 weeks (or the remaining weeks, whichever is greater)
            # Use max of 12 or remaining_weeks
            forecast_weeks = max(12, remaining_weeks)
            # print("forecast_weeks", forecast_weeks)
            forecast_df = forecast_sales_for_walmart(walmart_data, forecast_weeks=forecast_weeks)
            forecasted_data.append(forecast_df)

        #         forecasted values appended and sorted by walmart_id and period in asc order
        #     df_combined = pd.concat(forecasted_data)
        df_combined = pd.concat([df_temp] + forecasted_data)
        df_combined1 = df_combined.sort_values(by=['walmart_id', 'period']).reset_index(drop=True)

        #     if no of weeks available is less than 26 then flag it to 1
        weeks_ava = sales_data1.groupby("walmart_id")["period"].count().reset_index(name='weeks_available')
        weeks_ava['flag'] = weeks_ava['weeks_available'].apply(lambda x: 1 if x < 26 else 0)

        # get anchor value for current week
        anchor_df = df_combined1[df_combined1['period'] == start_date]
        logger.info("get_sales_data completed successfully.")
        return anchor_df, weeks_ava
    except Exception as e:
        logger.error(f"Error occurred in get_sales_data: {e}", exc_info=True)
        raise


def get_walmart_buybox_data(project_id, start_date):
    """
    Retrieves and processes Walmart buy box data for a specific date.
    
    This function queries the CC_I2O_DATA_MART database for Walmart marketplace
    buy box information, filters it for a specified date, and transforms the 
    dataset to include only relevant columns with renamed headers.
    
    Parameters:
    ----------
    project_id : str
        The ID of the project used for database connection/authentication.
    start_date : datetime or str
        The specific date for which to filter buy box data.
        
    Returns:
    -------
    pandas.DataFrame
        A DataFrame containing filtered buy box data with the following columns:
        - walmart_id: Product identifier (from product_code)
        - seller_name: Name of the winning seller on the buy box
        - buy_box_percent: Percentage of time the seller won the buy box
        - period: The date of the data
    """
    logger.info("get_buy box_data ")
    bb_query = "Select * from CC_I2O_DATA_MART.viz_listing_observed_buy_box WHERE marketplace='Walmart' and reporting_range='Weekly'"
    bb_data=get(bb_query,project_id)
    bb_data['period'] = pd.to_datetime(bb_data['period'])
    bb_data1=bb_data[bb_data['period']== start_date]
    bb_data2=bb_data1[['product_code','Buy_box_winning_seller','observed_buy_box','period']]
    bb_data2.columns=['walmart_id','seller_name','buy_box_percent','period']
    logger.info("get_buy box_data ")
    return bb_data2


def main(project_id, start_date):
    """
   Main function that orchestrates the calculation of lost sales for Walmart products.
   
   This function processes data through several steps:
   1. Retrieves buy box performance data using get_walmart_buybox_data()
   2. Fetches sales data and data availability metrics using get_sales_data()
   3. Merges these datasets to calculate lost sales based on buy box performance
   4. Handles missing values by substituting with period average when needed
   5. Adds data quality flags and formats the result for output
   6. Writes the final results to BigQuery
   
   Parameters:
   ----------
   project_id : str
       The ID of the project used for database connections and output storage.
   start_date : datetime or str
       The specific date for which to calculate lost sales metrics.
       
   Returns:
   -------
   None
       The function does not return a value but writes the result to BigQuery
       using the write_output_to_bq() function.
       
   Output Data:
   -----------
   The final output dataset contains the following columns:
   - walmart_id: Product identifier
   - seller_name: Name of the seller on the buy box
   - buy_box_percent: Percentage of time the seller won the buy box
   - period: The analysis date (formatted as YYYY-MM-DD)
   - anchor: Sales value for the product
   - lost_sales: Calculated lost sales (anchor * buy_box_percent / 100)
   - flag: Indicator for products with insufficient historical data
   - report_run_date: Execution date of the report (current date)

   """
    # get buy box data
    bb_df = get_walmart_buybox_data(project_id, start_date)
    bb_df['walmart_id'] = bb_df['walmart_id'].astype(str)
    # get sales data
    sales_df, weeksava_df = get_sales_data(project_id, start_date)
    sales_df['walmart_id'] = sales_df['walmart_id'].astype(str)

    #     merging=pd.merge(bb_df,sales_df[['walmart_id','sales']],on=['walmart_id','period'],how='left')
    merging = pd.merge(bb_df, sales_df[['walmart_id', 'sales', 'period']], on=['walmart_id', 'period'], how='left')

    merging.rename(columns={'sales': 'anchor'}, inplace=True)
    # ----handle the case when sales data not present for any walmart isd then take avg of anchor value of
    # ---all avaialble walmart id for that period and substitute by that value
    # Calculate the mean anchor per walmart_id for the period 2025-03-30
    mean_anchor = merging[merging['period'] == start_date].groupby('period')['anchor'].mean()

    # Replace NaN values in the 'anchor' column with the mean anchor for all walmart_id anchor value mean

    merging['anchor'] = np.where(pd.isna(merging['anchor']), mean_anchor, merging['anchor'])
    # calculate lost sales
    merging['lost_sales'] = np.round(merging['anchor'] * (merging['buy_box_percent'] / 100), 2)

    # add flag column indicating IDs having less than 26 week sales data
    weeksava_df['walmart_id'] = weeksava_df['walmart_id'].astype(str)

    final = pd.merge(merging, weeksava_df, on='walmart_id', how='left')
    final1 = final[['walmart_id', 'seller_name', 'buy_box_percent', 'period',
                    'anchor', 'lost_sales', 'flag']]

    final1.columns = ['walmart_id', 'seller_name', 'buy_box_percent', 'period',
                      'anchor', 'lost_sales', 'flag']
    result_order = ['walmart_id', 'seller_name',
                    'buy_box_percent', 'period', 'anchor', 'lost_sales', 'flag']
    result = final1[result_order]
    current_date = pd.to_datetime(datetime.today().date())
    result['report_run_date'] = current_date
    result['report_run_date'] = result['report_run_date'].dt.strftime('%Y-%m-%d')
    result['period'] = result['period'].dt.strftime('%Y-%m-%d')

    write_output_to_bq(result, project_id)

def get_memory_usage():
    """Get current memory usage in MB"""
    rusage = resource.getrusage(resource.RUSAGE_SELF)
    # Convert from KB to MB
    return rusage.ru_maxrss / 1024  

@functions_framework.http
def hello_http(request):
    
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    start_time = time.time()
    start_memory = get_memory_usage()
    logging.info(f"Starting memory usage: {start_memory:.2f} MB")

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'projectId' in request_json:
        project_id = request_json['projectId']
        start_date = request_json['startDate']

    else:
        return "Input incorrect"

    main(project_id,start_date)
    # Calculate total execution time
    execution_time = time.time() - start_time
    
    logging.info(f"Total execution time: {execution_time:.2f} seconds")

       # Record ending memory
    end_memory = get_memory_usage()
    memory_used = end_memory - start_memory
    
    logging.info(f"Final memory usage: {end_memory:.2f} MB")
    logging.info(f"Memory increase during execution: {memory_used:.2f} MB")
    
    # Return execution time along with success message
    result = {
        "status": "Successfully Finished",
        "execution_time_seconds": round(execution_time, 2),
        "memory_increase_mb": round(memory_used, 2)
    }
    return result

