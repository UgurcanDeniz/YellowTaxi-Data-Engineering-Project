import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    df = df.drop_duplicates().reset_index(drop=True)
    df['operation_id'] = df.index

    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year

    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year',
                             'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year']]
    
    pickup_location_dim = df[['pickup_longitude','pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_longitude','pickup_latitude']]
    
    
    dropoff_location_dim = df[['dropoff_longitude','dropoff_latitude']].reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_longitude','dropoff_latitude']]
    
    
    rate_code_type = {
       1:"Standard rate",
       2:"JFK",
       3:"Newark",
       4:"Nassau or Westchester",
       5:"Negotiated fare",
       6:"Group ride"
    }

    rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]
    
    payment_type = {
       1:"Credit card",
       2:"Cash",
       3:"No charge",
       4:"Dispute",
       5:"Unknown",
       6:"Voided trip"
    }

    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]
    
    vendor_type = {
       1:"Creative Mobile Technologies, LLC",
       2:"VeriFone Inc."
    }

    vendor_dim = df[['VendorID']].reset_index(drop=True)
    vendor_dim['vendor_id'] = vendor_dim.index
    vendor_dim['vendor_name'] = vendor_dim['VendorID'].map(vendor_type)
    vendor_dim = vendor_dim[['vendor_id','VendorID','vendor_name']]
    
    fact_table = df.merge(vendor_dim, left_on='operation_id', right_on='vendor_id') \
             .merge(rate_code_dim, left_on='operation_id', right_on='rate_code_id') \
             .merge(pickup_location_dim, left_on='operation_id', right_on='pickup_location_id') \
             .merge(dropoff_location_dim, left_on='operation_id', right_on='dropoff_location_id') \
             .merge(datetime_dim, left_on='operation_id', right_on='datetime_id') \
             .merge(payment_type_dim, left_on='operation_id', right_on='payment_type_id') \
             [['operation_id','vendor_id', 'datetime_id', 'pickup_location_id', 
               'dropoff_location_id', 'rate_code_id', 'payment_type_id', 'store_and_fwd_flag', 
               'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 
               'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']]
    
    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "rate_code_dim":rate_code_dim.to_dict(orient="dict"),
    "pickup_location_dim":pickup_location_dim.to_dict(orient="dict"),
    "dropoff_location_dim":dropoff_location_dim.to_dict(orient="dict"),
    "payment_type_dim":payment_type_dim.to_dict(orient="dict"),
    "vendor_dim":vendor_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
