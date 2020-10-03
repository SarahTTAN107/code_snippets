-- some snippets for handling BQ
-- WITH statement

WITH visitors AS
  (
  SELECT 
    COUNT(DISTINCT fullVisitorID) AS total_visitors
  FROM 
  `data-to-insights.ecommerce.web_analytics`
  ),

purchasers AS 
  (
  SELECT 
    COUNT(DISTINCT fullVisitorID) AS total_purchasers
  FROM 
    `data-to-insights.ecommerce.web_analytics` 
  WHERE
    totals.transactions IS NOT NULL
    )

select 
  total_visitors, 
  total_purchasers, 
  total_purchasers/total_visitors as conversion_rate 
from 
  visitors, purchasers; 

