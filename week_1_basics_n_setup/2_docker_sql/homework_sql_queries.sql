--- Find number of trips on 18.09.2019

SELECT
	count(gtd.index)
FROM
	green_trip_data gtd
WHERE
	gtd.lpep_pickup_datetime::date = '2019-09-18'
	AND gtd.lpep_dropoff_datetime::date = '2019-09-18';

--- Which was the pick up day with the largest trip distance?

SELECT
	gtd.lpep_pickup_datetime
FROM
	green_trip_data gtd
ORDER BY
	gtd.trip_distance DESC
LIMIT 1;

/* Which were the 3 pick up Boroughs on 18.09.2019 
 * that had a sum of total_amount superior to 50000?*/ 
SELECT
	t."Borough",
	sum(t.total_amount) AS total_sum
FROM
	(
	SELECT
		*
	FROM
		green_trip_data gtd
	INNER JOIN zones z ON
		gtd."PULocationID" = z."LocationID"
	WHERE
		gtd.lpep_pickup_datetime::date = '2019-09-18'
		AND gtd.lpep_dropoff_datetime::date = '2019-09-18') t
GROUP BY
	t."Borough"
ORDER BY
	total_sum DESC
LIMIT 3;

/*For the passengers picked up in September 2019 in the zone name Astoria,
 * which was the drop off zone that had the largest tip? 
 * We want the name of the zone, not the id.*/
SELECT
	t.do_zone
FROM
	(
	SELECT
		*
	FROM
		green_trip_data gtd
	INNER JOIN (
		SELECT
			z."Borough" AS pu_borough,
			z."Zone" AS pu_zone,
			z."LocationID" AS pu_location_id
		FROM
			zones z) zpu ON
		gtd."PULocationID" = zpu.pu_location_id
	INNER JOIN (
		SELECT
			z."Borough" AS do_borough,
			z."Zone" AS do_zone,
			z."LocationID" AS do_location_id
		FROM
			zones z) zdo ON
		gtd."DOLocationID" = zdo.do_location_id) t
WHERE
	t.pu_zone = 'Astoria'
ORDER BY
	t.tip_amount DESC
LIMIT 1;

