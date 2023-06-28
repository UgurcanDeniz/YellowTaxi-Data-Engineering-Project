CREATE OR REPLACE TABLE yellow-taxi-project-389405.yellowtaxi_data_engineering.tbl_analytics AS (
SELECT 
a.operation_id,
b.vendor_name,
c.tpep_pickup_datetime,
c.tpep_dropoff_datetime,
d.pickup_longitude,
d.pickup_latitude,
e.dropoff_longitude,
e.dropoff_latitude,
f.rate_code_name,
g.payment_type_name,
a.store_and_fwd_flag,
a.passenger_count,
a.trip_distance,
a.fare_amount,
a.extra,
a.mta_tax,
a.tip_amount,
a.tolls_amount,
a.improvement_surcharge,
a.total_amount,
FROM

yellow-taxi-project-389405.yellowtaxi_data_engineering.fact_table a
JOIN yellow-taxi-project-389405.yellowtaxi_data_engineering.vendor_dim b ON a.vendor_id = b.vendor_id
JOIN yellow-taxi-project-389405.yellowtaxi_data_engineering.datetime_dim c ON a.datetime_id = c.datetime_id
JOIN yellow-taxi-project-389405.yellowtaxi_data_engineering.pickup_location_dim d  ON a.pickup_location_id = d.pickup_location_id
JOIN yellow-taxi-project-389405.yellowtaxi_data_engineering.dropoff_location_dim e ON a.dropoff_location_id = e.dropoff_location_id
JOIN yellow-taxi-project-389405.yellowtaxi_data_engineering.rate_code_dim f ON a.rate_code_id = f.rate_code_id
JOIN yellow-taxi-project-389405.yellowtaxi_data_engineering.payment_type_dim g ON a.payment_type_id = g.payment_type_id )
;
