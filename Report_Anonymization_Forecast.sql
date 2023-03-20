-- Declare all regex matching months for which we can forecast anonymization based on anonymization period
-- Example : n36_May21 matches all months for which we have data to anonymize on for the month of May 2021
  DECLARE n12_Oct21 STRING DEFAULT "2021_09|2021_08|2021_07|2021_06|2021_05|2021_04|2021_03|2021_02|2021_01|2020_12|2020_11|2020_10";
  DECLARE n12_Nov21 STRING DEFAULT "2021_10|2021_09|2021_08|2021_07|2021_06|2021_05|2021_04|2021_03|2021_02|2021_01|2020_12|2020_11";
  DECLARE n12_Dec21 STRING DEFAULT "2021_11|2021_10|2021_09|2021_08|2021_07|2021_06|2021_05|2021_04|2021_03|2021_02|2021_01|2020_12";
  DECLARE n12_Jan22 STRING DEFAULT "2021_12|2021_11|2021_10|2021_09|2021_08|2021_07|2021_06|2021_05|2021_04|2021_03|2021_02|2021_01";
  DECLARE n12_Feb22 STRING DEFAULT "2021_12|2021_11|2021_10|2021_09|2021_08|2021_07|2021_06|2021_05|2021_04|2021_03|2021_02";
  DECLARE n12_Mar22 STRING DEFAULT "2021_12|2021_11|2021_10|2021_09|2021_08|2021_07|2021_06|2021_05|2021_04|2021_03";
  DECLARE n12_Apr22 STRING DEFAULT "2021_12|2021_11|2021_10|2021_09|2021_08|2021_07|2021_06|2021_05|2021_04";
  DECLARE n12_May22 STRING DEFAULT "2021_12|2021_11|2021_10|2021_09|2021_08|2021_07|2021_06|2021_05";
  DECLARE n12_Jun22 STRING DEFAULT "2021_12|2021_11|2021_10|2021_09|2021_08|2021_07|2021_06";
  DECLARE n12_Jul22 STRING DEFAULT "2021_12|2021_11|2021_10|2021_09|2021_08|2021_07";
  DECLARE n12_Aug22 STRING DEFAULT "2021_12|2021_11|2021_10|2021_09|2021_08";
  DECLARE n12_Sep22 STRING DEFAULT "2021_12|2021_11|2021_10|2021_09";
  DECLARE n12_Oct22 STRING DEFAULT "2021_12|2021_11|2021_10";
  DECLARE n12_Nov22 STRING DEFAULT "2021_12|2021_11";
  DECLARE n12_Dec22 STRING DEFAULT "2021_12";


WITH tracking_log_tmp AS (
  SELECT
    OCD_CONTACT_MASTER_ID as ocdMasterId,
    FORMAT_DATETIME("%Y_%m", DATETIME (TRACKINGLOG_DATE)) as YYYY_mm,
    SUM(CASE WHEN TRACKINGLOG_URL_TYPE="Open" THEN 1 ELSE 0 END) AS OpenedEmail,
  FROM `emea-c1-dwh-prd.switzerland_all.tracking_log`
  GROUP BY 1,2 ORDER BY 1),

 -- COUNT sample and Product testing from interaction table
  Own_ProductSampleandTest AS (
  SELECT  
    FORMAT_DATETIME("%Y_%m", DATETIME (int.interaction.interactionDate)) as YYYY_mm,
    int.interaction.ocdContactMasterId,
    int.context.ocdInteractionId,
    MAX(IF(attributes.attributeType = "Nature", attributes.attributeValue, NULL)) AS OP_Nature,
    MAX(IF(attributes.attributeType = "OperationName", attributes.attributeValue, NULL)) AS OP_Operation,
    MAX(IF(attributes.attributeType = "EANCode", attributes.attributeValue, NULL)) AS OP_EAN,
  FROM `switzerland_all.interaction` int, UNNEST( int.interaction.attributes) attributes
  WHERE REGEXP_CONTAINS(int.interaction.actionType, "Own_Product|Own_Sample|Test_Product")
  GROUP BY 1,2,3),
  
  SampleTest_tmp AS (
  SELECT
    YYYY_mm,
    ocdContactMasterId as ocdMasterId,
    Count(distinct ocdInteractionID) as nSample
  FROM Own_ProductSampleandTest
  GROUP BY 1,2),
  
    -- COUNT orders from orders table
  Orders_tmp AS (
  SELECT
    DISTINCT OrderId,
    ocdMasterID,
    OrderDate,    
  FROM (
    SELECT
      a.context.ocdTicketId AS OrderId,
      a.header.ocdContactMasterId AS ocdMasterId,
      a.header.ticketDate AS OrderDate,
    FROM
      `emea-c1-dwh-prd.switzerland_all.sales_history` a
    WHERE
      a.validPurchase
    GROUP BY 1,2,3
    UNION ALL
    SELECT
      b.context.ocdOrderId AS OrderId,
      b.header.ocdContactMasterId AS ocdMasterId,
      b.header.orderDate AS OrderDate
    FROM
      `emea-c1-dwh-prd.switzerland_all.order` b
    WHERE
      b.validPurchase
    GROUP BY 1,2,3 ) ),
    
  CountOrders_tmp AS (
  SELECT
    FORMAT_DATETIME("%Y_%m", DATETIME (OrderDate)) as YYYY_mm,
    ocdMasterID,
    Count(distinct orderID) as nOrders
  FROM Orders_tmp
  GROUP BY 1,2),
  
   -- COUNT Abandonned Carts for logged users
  AbandonnedCart_tmp AS (
  SELECT  
    FORMAT_DATETIME("%Y_%m", DATETIME (header.cartLastModificationDate)) as YYYY_mm,
    header.ocdContactMasterId as ocdMasterId,
    Count(distinct context.ocdCartID) as nCarts
  FROM `switzerland_all.cart`
  GROUP BY 1,2),
    
  helpYYYY_MM AS (
  SELECT DISTINCT ocdMasterId, YYYY_mm
  FROM ( 
      SELECT ocdMasterId, YYYY_mm FROM tracking_log_tmp UNION ALL
      SELECT ocdMasterId, YYYY_mm FROM CountOrders_tmp UNION ALL
      SELECT ocdMasterId, YYYY_mm FROM SampleTest_tmp UNION ALL
      SELECT ocdMasterId, YYYY_mm FROM AbandonnedCart_tmp) ),
  
  final_tmp as (SELECT
  division,
  contextMaster.brand,
  m.ocdMasterID,
  help.YYYY_mm,
  SUM(CASE WHEN tracking.OpenedEmail>0 THEN 1 ELSE 0 END) as Openers,
  SUM(CASE WHEN samples.nSample>0 THEN 1 ELSE 0 END) as Samplers,
  SUM(CASE WHEN carts.nCarts>0 THEN 1 ELSE 0 END) as CartAbandonners,
  SUM(CASE WHEN orders.nOrders>0 THEN 1 ELSE 0 END) as Customers,
  SUM(CASE WHEN tracking.OpenedEmail>0 OR samples.nSample>0 OR orders.nOrders>0 OR orders.nOrders>0 THEN 1 ELSE 0 END) as Engaged,

  FROM `emea-c1-dwh-prd.switzerland_all.contact_master` m
  LEFT JOIN helpYYYY_MM as help on help.ocdMasterID = m.contextMaster.ocdMasterId
  LEFT JOIN tracking_log_tmp tracking on (tracking.ocdMasterId = m.ocdMasterId AND tracking.YYYY_mm = help.YYYY_mm)
  LEFT JOIN CountOrders_tmp orders on (orders.ocdMasterId = m.ocdMasterId AND orders.YYYY_mm = help.YYYY_mm)
  LEFT JOIN SampleTest_tmp samples on (samples.ocdMasterId = m.ocdMasterId AND samples.YYYY_mm = help.YYYY_mm)
  LEFT JOIN AbandonnedCart_tmp carts on (carts.ocdMasterId = m.ocdMasterId AND carts.YYYY_mm = help.YYYY_mm)

  WHERE not genericContact
  AND not anonymizedContact
  AND not cCareNonOptinContact
  AND division <> "Consumer Products Division"
  AND acquisitionSourcePlatform<> "customercare"

  GROUP BY 1,2,3,4
  ORDER BY 1,2,3,4),  
  
  bool_tmp as (
  SELECT 
  division,
  brand,
  ocdMasterID,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Oct21) THEN Engaged END) as n12_Oct21,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Nov21) THEN Engaged END) as n12_Nov21,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Dec21) THEN Engaged END) as n12_Dec21,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Jan22) THEN Engaged END) as n12_Jan22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Feb22) THEN Engaged END) as n12_Feb22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Mar22) THEN Engaged END) as n12_Mar22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Apr22) THEN Engaged END) as n12_Apr22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_May22) THEN Engaged END) as n12_May22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Jun22) THEN Engaged END) as n12_Jun22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Jul22) THEN Engaged END) as n12_Jul22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Aug22) THEN Engaged END) as n12_Aug22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Sep22) THEN Engaged END) as n12_Sep22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Oct22) THEN Engaged END) as n12_Oct22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Nov22) THEN Engaged END) as n12_Nov22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,n12_Dec22) THEN Engaged END) as n12_Dec22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2020_09") THEN Engaged END) as c12_Oct21,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2020_10") THEN Engaged END) as c12_Nov21,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2020_11") THEN Engaged END) as c12_Dec21,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2020_12") THEN Engaged END) as c12_Jan22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_01") THEN Engaged END) as c12_Feb22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_02") THEN Engaged END) as c12_Mar22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_03") THEN Engaged END) as c12_Apr22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_04") THEN Engaged END) as c12_May22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_05") THEN Engaged END) as c12_Jun22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_06") THEN Engaged END) as c12_Jul22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_07") THEN Engaged END) as c12_Aug22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_08") THEN Engaged END) as c12_Sep22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_09") THEN Engaged END) as c12_Oct22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_10") THEN Engaged END) as c12_Nov22,
  SUM(CASE WHEN REGEXP_CONTAINS(YYYY_mm,"2021_11") THEN Engaged END) as c12_Dec22,
  from final_tmp
  GROUP BY 1,2,3)
  
  SELECT
  b.division,
  b.brand,
  COUNT(distinct CASE WHEN n12_Oct21=0 AND m.contactAcquisitionDate < "2021-11-01" THEN b.ocdMasterID END) AS Anon_Oct21,
  COUNT(distinct CASE WHEN n12_Oct21=0 AND m.isContactable AND m.contactAcquisitionDate < "2021-11-01" THEN b.ocdMasterID END ) AS AnonContactable_Oct21,
  COUNT(distinct CASE WHEN n12_Nov21=0 AND (c12_Nov21=1 OR m.contactAcquisitionDate < "2021-12-01") THEN b.ocdMasterID END) AS Anon_Nov21,
  COUNT(distinct CASE WHEN n12_Nov21=0 and m.isContactable AND (c12_Nov21=1 OR m.contactAcquisitionDate < "2021-12-01") THEN b.ocdMasterID END ) AS AnonContactable_Nov21,
  COUNT(distinct CASE WHEN n12_Dec21=0 AND (c12_Dec21=1 OR m.contactAcquisitionDate < "2021-01-01") THEN b.ocdMasterID END) AS Anon_Dec21,
  COUNT(distinct CASE WHEN n12_Dec21=0 and m.isContactable AND (c12_Dec21=1 OR m.contactAcquisitionDate < "2021-01-01") THEN b.ocdMasterID END ) AS AnonContactable_Dec21,
  COUNT(distinct CASE WHEN n12_Jan22=0 AND (c12_Jan22=1 OR m.contactAcquisitionDate < "2021-02-01") THEN b.ocdMasterID END) AS Anon_Jan22,
  COUNT(distinct CASE WHEN n12_Jan22=0 and m.isContactable AND (c12_Jan22=1 OR m.contactAcquisitionDate < "2021-02-01") THEN b.ocdMasterID END ) AS AnonContactable_Jan22,
  COUNT(distinct CASE WHEN n12_Feb22=0 AND (c12_Feb22=1 OR m.contactAcquisitionDate < "2021-03-01") THEN b.ocdMasterID END) AS Anon_Feb22,
  COUNT(distinct CASE WHEN n12_Feb22=0 and m.isContactable AND (c12_Feb22=1 OR m.contactAcquisitionDate < "2021-03-01") THEN b.ocdMasterID END ) AS AnonContactable_Feb22,
  COUNT(distinct CASE WHEN n12_Mar22=0 AND (c12_Mar22=1 OR m.contactAcquisitionDate < "2021-04-01") THEN b.ocdMasterID END) AS Anon_Mar22,
  COUNT(distinct CASE WHEN n12_Mar22=0 and m.isContactable AND (c12_Mar22=1 OR m.contactAcquisitionDate < "2021-04-01") THEN b.ocdMasterID END ) AS AnonContactable_Mar22,
  COUNT(distinct CASE WHEN n12_Apr22=0 AND (c12_Apr22=1 OR m.contactAcquisitionDate < "2021-05-01") THEN b.ocdMasterID END) AS Anon_Apr22,
  COUNT(distinct CASE WHEN n12_Apr22=0 and m.isContactable AND (c12_Apr22=1 OR m.contactAcquisitionDate < "2021-05-01") THEN b.ocdMasterID END ) AS AnonContactable_Apr22,
  COUNT(distinct CASE WHEN n12_May22=0 AND (c12_May22=1 OR m.contactAcquisitionDate < "2021-06-01") THEN b.ocdMasterID END) AS Anon_May22,
  COUNT(distinct CASE WHEN n12_May22=0 and m.isContactable AND (c12_May22=1 OR m.contactAcquisitionDate < "2021-06-01") THEN b.ocdMasterID END ) AS AnonContactable_May22,
  COUNT(distinct CASE WHEN n12_Jun22=0 AND (c12_Jun22=1 OR m.contactAcquisitionDate < "2021-07-01") THEN b.ocdMasterID END) AS Anon_Jun22,
  COUNT(distinct CASE WHEN n12_Jun22=0 and m.isContactable AND (c12_Jun22=1 OR m.contactAcquisitionDate < "2021-07-01") THEN b.ocdMasterID END ) AS AnonContactable_Jun22,
  COUNT(distinct CASE WHEN n12_Jul22=0 AND (c12_Jul22=1 OR m.contactAcquisitionDate < "2021-08-01") THEN b.ocdMasterID END) AS Anon_Jul22,
  COUNT(distinct CASE WHEN n12_Jul22=0 and m.isContactable AND (c12_Jul22=1 OR m.contactAcquisitionDate < "2021-08-01") THEN b.ocdMasterID END ) AS AnonContactable_Jul22,
  COUNT(distinct CASE WHEN n12_Aug22=0 AND (c12_Aug22=1 OR m.contactAcquisitionDate < "2021-09-01") THEN b.ocdMasterID END) AS Anon_Aug22,
  COUNT(distinct CASE WHEN n12_Aug22=0 and m.isContactable AND (c12_Aug22=1 OR m.contactAcquisitionDate < "2021-09-01") THEN b.ocdMasterID END ) AS AnonContactable_Aug22,
  COUNT(distinct CASE WHEN n12_Sep22=0 AND (c12_Sep22=1 OR m.contactAcquisitionDate < "2021-10-01") THEN b.ocdMasterID END) AS Anon_Sep22,
  COUNT(distinct CASE WHEN n12_Sep22=0 and m.isContactable AND (c12_Sep22=1 OR m.contactAcquisitionDate < "2021-10-01") THEN b.ocdMasterID END ) AS AnonContactable_Sep22,
  COUNT(distinct CASE WHEN n12_Oct22=0 AND (c12_Oct22=1 OR m.contactAcquisitionDate < "2021-11-01") THEN b.ocdMasterID END) AS Anon_Oct22,
  COUNT(distinct CASE WHEN n12_Oct22=0 and m.isContactable AND (c12_Oct22=1 OR m.contactAcquisitionDate < "2021-11-01") THEN b.ocdMasterID END ) AS AnonContactable_Oct22,
  COUNT(distinct CASE WHEN n12_Nov22=0 AND (c12_Nov22=1 OR m.contactAcquisitionDate < "2021-12-01") THEN b.ocdMasterID END) AS Anon_Nov22,
  COUNT(distinct CASE WHEN n12_Nov22=0 and m.isContactable AND (c12_Nov22=1 OR m.contactAcquisitionDate < "2021-12-01") THEN b.ocdMasterID END ) AS AnonContactable_Nov22,
  COUNT(distinct CASE WHEN n12_Dec22=0 AND (c12_Dec22=1 OR m.contactAcquisitionDate < "2020-12-01") THEN b.ocdMasterID END) AS Anon_Dec22,
  COUNT(distinct CASE WHEN n12_Dec22=0 and m.isContactable AND (c12_Dec22=1 OR m.contactAcquisitionDate < "2020-12-01") THEN b.ocdMasterID END ) AS AnonContactable_Dec22
    from bool_tmp b
  INNER JOIN `emea-c1-dwh-prd.switzerland_all.contact_master` m ON m.contextMaster.ocdMasterId = b.ocdMasterID
  GROUP BY 1,2
  
  




