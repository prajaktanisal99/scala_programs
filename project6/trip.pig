TRIP = LOAD '$T' USING PigStorage(',') AS (VendorID:chararray,tpep_pickup_datetime:chararray,tpep_dropoff_datetime:chararray,passenger_count:chararray,trip_distance:double,pickup_longitude:chararray,pickup_latitude:chararray,RateCodeID:chararray,store_and_fwd_flag:chararray,dropoff_longitude:chararray,dropoff_latitude:chararray,payment_type:chararray,fare_amount:chararray,extra:chararray,mta_tax:chararray,tip_amount:chararray,tolls_amount:chararray,improvement_surcharge:chararray,amount:double);

TRIP = FILTER TRIP BY NOT ($0 MATCHEs '.*VendorID.*');
PARSED_DATA = FOREACH TRIP GENERATE (int)ROUND(trip_distance) as  distance, amount;
PARSED_DATA = FILTER PARSED_DATA BY distance < 200;

GROUPED_DATA = GROUP PARSED_DATA BY distance;

AVERAGE_DATA = FOREACH GROUPED_DATA {
    total_amount = SUM(PARSED_DATA.amount);
    count_of_rides = COUNT(PARSED_DATA.amount);
    mean = total_amount / count_of_rides;
    GENERATE group AS distance, mean AS AMOUNT_MEAN;
}

DUMP AVERAGE_DATA;