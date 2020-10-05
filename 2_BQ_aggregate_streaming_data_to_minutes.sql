-- example of BQ's StandardSQL to aggregate IoT data to minute. check further details in this qwiklabs: https://googlecoursera.qwiklabs.com/focuses/11579147?parent=lti_session
WITH streaming_data AS (

SELECT 
  timestamp, 
  TIMESTAMP_TRUNC(timestamp, HOUR, 'UTC') AS hour, 
  TIMESTAMP_TRUNC(timestamp, MINUTE, 'UTC') AS minute,
  TIMESTAMP_TRUNC(timestamp, SECOND, 'UTC') AS second,
  ride_id,
  latitude,
  longitude,
  meter_reading,
  ride_status,
  passenger_count
FROM 
  `taxirides.realtime` 
WHERE
  ride_status = 'dropoff'
ORDER BY
  timestamp DESC
LIMIT 100000

) 

SELECT 
ROW_NUMBER() OVER() AS dashboard_sort, 
minute,
COUNT(DISTINCT ride_id) AS total_ride, 
SUM(meter_reading) AS total_revenue,
SUM(passenger_count) AS total_passengers
FROM streaming_data 
GROUP BY minute, timestamp
