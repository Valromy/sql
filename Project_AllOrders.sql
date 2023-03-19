DECLARE startDate DATE DEFAULT "2000-01-01";
DECLARE endDate DATE DEFAULT "2023-03-18";
DECLARE brands STRING DEFAULT "Kiehls";
DECLARE factor FLOAT64 DEFAULT 0.8;

-- CTE 1: OFFLINE orders with CustomerID, OrderID and ItemID
-- hence duplicated lines for CustomerID and OrderID 
WITH offline_orders AS (
  SELECT distinct
    SHA256(CAST(header.ocdContactMasterId AS STRING)) AS CustomerId,
    -- order level
    'offline' AS sourceChannel,
    SHA256(CAST(context.ocdTicketId AS STRING)) AS OrderId,
    header.ticketDate AS OrderDate,
    header.purchaseAmountTaxIncludedBeforeDiscount * factor as orderAmountBeforeDiscount,
    header.purchaseAmountTaxIncludedAfterDiscount * factor as orderAmount,
    IF(header.purchaseAmountTaxIncludedBeforeDiscount = header.purchaseAmountTaxIncludedAfterDiscount, 0, 1) AS orderPromo,

    -- ean level
    hits.eanCode,
    hits.itemQuantity,
    hits.lineAmountTaxIncludedBeforeDiscount * factor as lineAmountBeforeDiscount,
    hits.lineAmountTaxIncludedAfterDiscount * factor as lineAmount,
    IF(hits.lineAmountTaxIncludedBeforeDiscount = hits.lineAmountTaxIncludedAfterDiscount, 0, 1) AS linePromo


  FROM `emea-c1-dwh-prd.switzerland_all.sales_history`
  CROSS JOIN UNNEST(lines) as hits 
  WHERE validPurchase and REGEXP_CONTAINS(context.brand, brands)
  and date(header.ticketDate) >= startDate
  and date(header.ticketDate) <= endDate
  and hits.lineAmountTaxIncludedBeforeDiscount > 0
),

-- CTE 2: ONLINE orders with CustomerID, OrderID and ItemID
-- hence duplicated lines for CustomerID and OrderID 
online_orders AS (
  SELECT distinct
    SHA256(CAST(header.ocdContactMasterId AS STRING)) AS CustomerId,
    -- order level
    'online' AS sourceChannel,
    SHA256(CAST(context.ocdOrderId AS STRING)) AS OrderId,
    header.orderDate AS OrderDate,
    header.merchandizeTotalAmount.merchandizeTotalAmountTaxIncludedBeforeDiscount * factor as orderAmountBeforeDiscount,
    header.merchandizeTotalAmount.merchandizeTotalAmountTaxIncludedAfterDiscount * factor as orderAmount,
    IF(header.merchandizeTotalAmount.merchandizeTotalAmountTaxIncludedBeforeDiscount = header.merchandizeTotalAmount.merchandizeTotalAmountTaxIncludedAfterDiscount, 0, 1) AS orderPromo,


    -- ean level
    hits.eanCode,
    hits.itemQuantity,
    hits.lineAmountTaxIncludedBeforeDiscount * factor as lineAmountBeforeDiscount,
    hits.lineAmountTaxIncludedAfterDiscount * factor as lineAmount,
    
    IF(hits.lineAmountTaxIncludedBeforeDiscount = hits.lineAmountTaxIncludedAfterDiscount, 0, 1) AS linePromo


  FROM `emea-c1-dwh-prd.switzerland_all.order`
  CROSS JOIN UNNEST(lines) as hits
  WHERE validPurchase and REGEXP_CONTAINS(context.brand, brands)
  and date(header.orderDate) >= startDate
  and date(header.orderDate) <= endDate
  and hits.lineAmountTaxIncludedBeforeDiscount> 0
),

-- CTE 3: Unique OrderID per CustomerID, ranked OrderDate
dedup_orders AS (
  SELECT DISTINCT o.CustomerId, o.OrderId, o.OrderDate
  FROM (
    SELECT * FROM offline_orders
    UNION ALL
    SELECT * FROM online_orders
  ) o
  WHERE o.CustomerId IS NOT NULL
  GROUP BY 1, 2, 3
  ORDER BY 1, 3
),

-- CTE 4: Attach to each Order ID the time in the purchase path of CustomerID
-- OrderRank denotates whether Order was first, second or third order (etc ..)
OrderNumberDB AS (
SELECT DISTINCT CustomerId, OrderId, OrderDate, 
  ROW_NUMBER() OVER (PARTITION BY CustomerId ORDER BY OrderDate ASC) AS OrderRank
FROM dedup_orders 
ORDER BY 1, 3),


-- CTE 5 : Item information deduplicated at itemID level
-- Cleaning of unused or very low frequency item categories / subcategories
UniqueEANS AS (
  SELECT distinct
    eanCode, 
    FIRST_VALUE(category) OVER (PARTITION BY eanCode ORDER BY variantSalesOpenDate DESC) as category,
    FIRST_VALUE(subcategory) OVER (PARTITION BY eanCode ORDER BY variantSalesOpenDate DESC) as subcategory,
    FIRST_VALUE(franchise) OVER (PARTITION BY eanCode ORDER BY variantSalesOpenDate DESC) as franchise,  
  FROM (
    SELECT DISTINCT variantInfo.eanCode,
      variantInfo.variantSalesOpenDate,
      CASE 
        WHEN classification.classificationAxis IN ('Miscellaneous', 'Fragrance', 'MakeUp') THEN 'Skin Care'
        ELSE IFNULL(classification.classificationAxis, "Skin Care")
      END AS category,
      IFNULL(
        CASE 
          WHEN classification.classificationSubAxis IN ('Miscellaneous', 'Miscellaneous Cosmetic', 'Women Fragrance', 'Face Care Caring', 'Lip Makeup') THEN 'Face Care'
          WHEN classification.classificationSubAxis = 'Deodorant' THEN 'Deodorants'
          WHEN classification.classificationSubAxis IN ('Health Hygiene', 'Bath & Shower') THEN 'Soaps'
          WHEN classification.classificationSubAxis IN ('Hair Care', 'Styling', 'Other Hair') THEN 'Hair'
          WHEN classification.classificationSubAxis IN ('Face Care for Men', 'Face Cleansing for Men') THEN 'Men Skin Care'
          WHEN classification.classificationSubAxis = 'Face Care Cleansing' THEN 'Face Cleansing'
          ELSE classification.classificationSubAxis
        END,
        "Face Care"
      ) AS subcategory,
      IFNULL(classification.classificationSubBrand, "OtherBrand") AS franchise
    FROM `emea-c1-dwh-prd.switzerland_all.item`
    WHERE REGEXP_CONTAINS(context.brand, brands)
    AND NOT REGEXP_CONTAINS(variantInfo.eanCode, ',')
  )
)


SELECT o.CustomerId,
       o.sourceChannel,
       o.OrderId,
       o.OrderDate,
       o.orderAmountBeforeDiscount,
       o.orderAmount,
       o.orderPromo,
       a.orderRank,
       SHA256(o.eanCode) as lineID,
       o.lineAmountBeforeDiscount,
       o.lineAmount,
       o.linePromo,
       o.itemQuantity,
       e.category,
       e.subcategory,
       CONCAT('Franchise_', TO_BASE64(SHA256(CAST(e.franchise AS STRING)))) AS franchise
FROM (
  SELECT *
  FROM offline_orders
  UNION ALL
  SELECT *
  FROM online_orders
) o
JOIN UniqueEANS e ON o.eanCode = e.eanCode
JOIN OrderNumberDB a ON o.OrderId = a.OrderId

WHERE o.CustomerId IS NOT NULL
AND o.OrderId IS NOT NULL
AND o.eanCode IS NOT NULL

ORDER BY CustomerId, OrderId, OrderDate DESC
