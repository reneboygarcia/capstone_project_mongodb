-- Q3: Table of address_country and number of amenities and number of rooms
SELECT
  address_country,
  amenities,
  IF(amenities="['']", 0, ARRAY_LENGTH(CAST(SPLIT(amenities, ',') AS ARRAY<STRING>))) AS num_amenities,
  COUNT(1) AS num_room
FROM
  `sample_airbnb.listings_and_reviews`
WHERE 1=1
  AND '%Wifi%' NOT IN UNNEST(CAST(SPLIT(amenities, ',') AS ARRAY<STRING>))
  AND '%Internet%' NOT IN UNNEST(CAST(SPLIT(amenities, ',') AS ARRAY<STRING>))
GROUP BY
  address_country,
  amenities

ORDER BY
  num_room DESC
 