DECLARE brands STRING DEFAULT "Kiehl's";

-- CTE 1: Find the top device for each unique customer and date combination
WITH top_device AS (
SELECT distinct
    OCD_CONTACT_MASTER_ID as ocdMasterId, 
    log_date, 
    FIRST_VALUE(DEVICE) OVER (-- find the first (most common) device used by the customer on that date
      PARTITION BY OCD_CONTACT_MASTER_ID, log_date
      ORDER BY num_deliveries DESC -- order by number of deliveries (so we get the most common device first)
    ) AS Device
FROM (
  SELECT 
    OCD_CONTACT_MASTER_ID,
    DATE(TRACKINGLOG_DATE) as log_date, 
    DEVICE,
    COUNT(DISTINCT DELIVERY_ID_AC) as num_deliveries,
  FROM `emea-c1-dwh-prd.switzerland_all.tracking_log`
  WHERE REGEXP_CONTAINS(BRAND, brands)
  GROUP BY OCD_CONTACT_MASTER_ID, DATE(TRACKINGLOG_DATE), DEVICE
)
),

-- CTE 2: Aggregate tracking data by customer ID and delivery date
tracking AS (
 SELECT distinct
    OCD_CONTACT_MASTER_ID as ocdMasterId,
    DATE(TRACKINGLOG_DATE) as log_date,
    SUM(CASE WHEN TRACKINGLOG_URL_TYPE="Open" THEN 1 ELSE 0 END) AS OpenedEmail,
    SUM(CASE WHEN TRACKINGLOG_URL_TYPE="Email click" THEN 1 ELSE 0 END) AS ClickedEmail,
    SUM(CASE WHEN TRACKINGLOG_URL_TYPE="Opt-out" THEN 1 ELSE 0 END) AS OptoutEmail,
  FROM `emea-c1-dwh-prd.switzerland_all.tracking_log`
  WHERE REGEXP_CONTAINS(BRAND, brands)
  GROUP BY 1,2 ORDER BY 1)

-- Main query: join the two CTEs on customer ID and delivery date, and show the device used, email opens, clicks, and opt-outs
SELECT 
SHA256(top_device.ocdMasterId) AS CustomerID,
top_device.log_date, 
top_device.Device, 
tracking.OpenedEmail, 
tracking.ClickedEmail, 
tracking.OptoutEmail

FROM top_device
JOIN tracking ON top_device.ocdMasterId = tracking.ocdMasterId 
AND top_device.log_date = tracking.log_date

ORDER BY CustomerID
