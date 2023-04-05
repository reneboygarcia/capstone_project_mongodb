  -- Q6: Number of first reviews over time?

SELECT
  DATE(first_review) AS date_of_first_review,
  COUNT(1) AS num_first_review
FROM
  `sample_airbnb.listings_and_reviews`
WHERE
  1=1
  AND first_review IS NOT NULL
GROUP BY
  first_review
ORDER BY
  first_review ASC