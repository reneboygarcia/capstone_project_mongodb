-- Q5: What room types are present in the sample_airbnb.listingsAndReviews collection?
SELECT
  DISTINCT(room_type) AS room_type,
  COUNT(1) AS num,
  AVG(CAST(price AS FLOAT64)) AS avg_price

FROM
  `sample_airbnb.listings_and_reviews`

GROUP BY
    room_type
    
ORDER BY
    num DESC