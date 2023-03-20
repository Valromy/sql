-- select ((offline ∪ online) ∩ contact_master)
DECLARE startDate DATE DEFAULT "2021-01-01" ; 
DECLARE endDate DATE DEFAULT "2023-01-01" ; 
DECLARE brandRegex STRING DEFAULT "Lancome|Kiehls|Ysl|GiorgioArmani";

WITH offline_orders AS (
SELECT
  a.context.brand,
  a.context.ocdTicketId AS OrderId,
  a.header.ocdContactMasterId AS ocdMasterId,
  eanCode,
  a.header.ticketDate AS OrderDate,
  LOWER(source.sourceChannel) AS sourceChannel,
  a.header.purchaseAmountTaxIncludedAfterDiscount as CHF,
  SUM(itemQuantity) as UN
FROM `emea-c1-dwh-prd.switzerland_all.sales_history` a, UNNEST(lines) as hits 
WHERE a.validPurchase and REGEXP_CONTAINS(a.context.brand, brandRegex) and lineAmountTaxIncludedAfterDiscount	> 0
GROUP BY 1,2,3,4,5,6,7
),

online_orders AS (
SELECT
  b.context.brand,
  b.context.ocdOrderId AS OrderId,
  b.header.ocdContactMasterId AS ocdMasterId,
  eanCode,
  b.header.orderDate AS OrderDate,
  "online" AS sourceChannel,
  b.header.orderTotalAmountTaxIncluded AS CHF,
  SUM(itemQuantity) as UN 
FROM `emea-c1-dwh-prd.switzerland_all.order` b, UNNEST(lines) as hits
--filter for valid purchase and only finished goods/no GWP in basket avg calculation
WHERE b.validPurchase and REGEXP_CONTAINS(b.context.brand, brandRegex) and lineAmountTaxIncludedAfterDiscount	> 0
GROUP BY 1,2,3,4,5,6,7
),

-- Temp table with both offline and online orders
omni_orders AS (
SELECT
A.brand,
OrderId,
ocdMasterID,
OrderDate,
eanCode,
sourceChannel,
q.classification.classificationSignature,
q.classification.classificationSubBrand,
q.classification.classificationAxis,
q.classification.classificationSubAxis,
-- this to avoid not including Franchise for which the local table FranchiseCH has not been updated, in this case Franchise becomes equals to SubAxis
CASE WHEN l.FranchiseAgg IS NULL THEN q.classification.classificationSubAxis ELSE l.FranchiseAgg END as FranchiseAggregated,
CASE WHEN l.FranchiseAgg IS NULL THEN "OtherFranchise" ELSE l.FranchiseFilter END as FranchiseFiltering,
CHF,
UN,
FROM (SELECT * FROM offline_orders UNION ALL SELECT * FROM online_orders) A
LEFT JOIN `emea-c1-dwh-prd.switzerland_all.item` q ON eanCode = q.variantInfo.eanCode
LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.FranchiseCH` l ON  (l.brand = q.classification.classificationSignature AND l.classificationSubBrand = q.classification.classificationSubBrand AND l.classificationAxis = q.classification.classificationAxis AND l.classificationSubAxis= q.classification.classificationSubAxis)
),

-- temp table to have product dimensions at ocdMasterID level ONLY
classification_ocdMasterID AS (
SELECT
ocdMasterID,
classificationAxis as ClientAxis,
classificationSubAxis as ClientSubAxis,
classificationSubBrand as ClientSubBrand,
FranchiseAggregated as ClientFranchiseAgg,
FranchiseFiltering as ClientFranchiseFilter,
FROM omni_orders
WHERE sourceChannel in ("online", "offline")
  and DATE(OrderDate) >= startDate
  and DATE(OrderDate) <= endDate
GROUP BY 1,2,3,4,5,6
),

-- rank orderID by orderDate at ocdMasterID level
dedup_omni_orders AS (SELECT Distinct ocdMasterID, OrderId, OrderDate FROM omni_orders GROUP BY 1,2,3 ORDER BY 1,3),
OrderNumberDB AS (SELECT distinct OrderId, ROW_NUMBER() OVER (PARTITION BY ocdMasterID ORDER BY OrderDate ASC) AS OrderRank
FROM dedup_omni_orders),


-- temp table to have product dimensions at orderlevel 
-- orderRank being at order id level only for first 5 orders
byocdMasterID_byOrderRank AS (
SELECT
omni_orders.ocdMasterID,
classificationAxis as OrderAxis,
classificationSubAxis as OrderSubAxis,
classificationSubBrand as OrderSubBrand,
FranchiseAggregated as OrderFranchiseAgg,
FranchiseFiltering OrderFranchiseFilter,
a.OrderRank,
a.OrderId
FROM omni_orders
LEFT JOIN OrderNumberDB a ON omni_orders.OrderId = a.OrderId
WHERE sourceChannel in ("online", "offline")
  and DATE(omni_orders.OrderDate) >= startDate
  and DATE(omni_orders.OrderDate) <= endDate
GROUP BY 1,2,3,4,5,6,7,8
),

-- Table Consumer + all orders all grouped at consumer-related information level, not grouped at ocdMasterID  
all_orders AS ( 
SELECT
  b.contextMaster.brand,
  s.affiliationChain,
  CONCAT("c_", d.ClientAxis) as ClientAxis,
  CONCAT("c_", d.ClientFranchiseAgg) as ClientFranchiseAgg,
  CONCAT("c_", d.ClientFranchiseFilter) as ClientFranchiseFilter,
  CONCAT("o_", c.OrderAxis) as OrderAxis,
  CONCAT("o_", c.OrderFranchiseAgg) as OrderFranchiseAgg,
  CONCAT("o_", c.OrderFranchiseFilter) as OrderFranchiseFilter,
  Count(distinct a.OrderID) as NumberOfOrders,
  Count(distinct case when OrderRank = 1 THEN a.OrderID END) as PresenceIn_1_Purchase,
  Count(distinct case when OrderRank = 2 THEN a.OrderID END) as PresenceIn_2_Purchase,
  Count(distinct case when OrderRank = 3 THEN a.OrderID END) as PresenceIn_3_Purchase,
  Count(distinct case when OrderRank = 4 THEN a.OrderID END) as PresenceIn_4_Purchase,
  Count(distinct case when OrderRank = 5 THEN a.OrderID END) as PresenceIn_5_Purchase,
  
FROM omni_orders a
INNER JOIN `emea-c1-dwh-prd.switzerland_all.contact_master` b on a.ocdMasterId = b.ocdMasterId
LEFT JOIN byocdMasterID_byOrderRank c on a.orderID = c.orderID
LEFT JOIN classification_ocdMasterID d ON a.ocdMasterID = d.ocdMasterID
LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.StoreRefsCH` s ON s.affiliationStore = b.preferences.affiliationStore.affiliationStore
-- GROUP
WHERE b.nominativeContact
  and division <> "Consumer Products Division"
  and b.acquisitionSourcePlatform<> "customercare"
  and sourceChannel in ("online", "offline")
  and DATE(OrderDate) >= startDate
  and DATE(OrderDate) <= endDate
  -- have an affiliation store
  and CHAR_LENGTH(b.preferences.affiliationStore.affiliationStore)>2
  --get out Miscellaneous and blank axi out of analysis
  and REGEXP_CONTAINS(d.ClientAxis,"Fragrance|Hair|Hygiene|MakeUp|Skin Care")
  and REGEXP_CONTAINS(c.OrderAxis,"Fragrance|Hair|Hygiene|MakeUp|Skin Care")
 -- and c.OrderRank <=5

GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 1,2,3,4,5
)

Select * from all_orders
