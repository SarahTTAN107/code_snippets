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

   -- Identify duplicate rows
SELECT
  COUNT(*) AS num_duplicate_rows,
  *
FROM
  `data-to-insights.ecommerce.all_sessions_raw`
GROUP BY
  fullVisitorId,
  channelGrouping,
  time,
  country,
  city,
  totalTransactionRevenue,
  transactions,
  timeOnSite,
  pageviews,
  sessionQualityDim,
  date,
  visitId,
  type,
  productRefundAmount,
  productQuantity,
  productPrice,
  productRevenue,
  productSKU,
  v2ProductName,
  v2ProductCategory,
  productVariant,
  currencyCode,
  itemQuantity,
  itemRevenue,
  transactionRevenue,
  transactionId,
  pageTitle,
  searchKeyword,
  pagePathLevel1,
  eCommerceAction_type,
  eCommerceAction_step,
  eCommerceAction_option
HAVING
  num_duplicate_rows > 1;
  
  
  -- count number of unique visitors/ count unique values:
  SELECT
  COUNT(*) AS product_views,
  COUNT(DISTINCT fullVisitorId) AS unique_visitors
FROM
  `data-to-insights.ecommerce.all_sessions`;
