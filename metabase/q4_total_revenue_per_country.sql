-- Q4: What is the total revenue generated per country
SELECT
  
  address_country,
  SUM(CAST(price AS FLOAT64)) AS revenue

FROM
  `sample_airbnb.listings_and_reviews`

GROUP BY
    address_country

ORDER BY 
    revenue DESC
