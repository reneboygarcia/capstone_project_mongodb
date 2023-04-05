-- Q2: How many rooms have no certain amenities in listings_and_reviews?

SELECT
  address_country,
  COUNT(1) AS num_room

FROM
  `sample_airbnb.listings_and_reviews`

WHERE 1=1
-- YOU CAN ADD OR REMOVE AMENITIES 

  AND amenities NOT LIKE '%Wifi%'
  AND amenities NOT LIKE '%Internet%'

GROUP BY
  address_country
ORDER BY
  num_room DESC

