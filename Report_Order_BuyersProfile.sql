-- select ((offline ∪ online) ∩ contact_master)
DECLARE startDate DATE DEFAULT "2021-01-01" ; 
DECLARE startDateL12M DATE DEFAULT "2021-12-31" ; 
DECLARE endDate DATE DEFAULT "2022-12-31" ; 
DECLARE brandRegex STRING DEFAULT "Lancome|Kiehls|GiorgioArmani|Ysl";
-- "Lancome|Kiehls|GiorgioArmani|Ysl";

WITH offline_orders AS (
SELECT
  a.context.ocdTicketId AS OrderId,
  a.header.ocdContactMasterId AS ocdMasterId,
  eanCode,
  a.header.ticketDate AS OrderDate,
  LOWER(source.sourceChannel) AS sourceChannel
FROM `emea-c1-dwh-prd.switzerland_all.sales_history` a, UNNEST(lines) as hits 
WHERE a.validPurchase and REGEXP_CONTAINS(a.context.brand, brandRegex) and lineAmountTaxIncludedAfterDiscount	> 0
GROUP BY 1,2,3,4,5
),

online_orders AS (
SELECT
  b.context.ocdOrderId AS OrderId,
  b.header.ocdContactMasterId AS ocdMasterId,
  eanCode,
  b.header.orderDate AS OrderDate,
  "online" AS sourceChannel,
FROM `emea-c1-dwh-prd.switzerland_all.order` b, UNNEST(lines) as hits
--filter for valid purchase and only finished goods/no GWP in basket avg calculation
WHERE b.validPurchase and REGEXP_CONTAINS(b.context.brand, brandRegex) and lineAmountTaxIncludedAfterDiscount	> 0
GROUP BY 1,2,3,4,5
),

-- Temp table with both offline and online orders --- 
omni_orders AS (
SELECT distinct
ocdMasterID,
OrderId,
OrderDate,
sourcechannel,
q.classification.classificationSignature,
q.classification.classificationSubBrand,
q.classification.classificationAxis,
q.classification.classificationSubAxis,
-- this to avoid not including Franchise for which the local table FranchiseCH has not been updated, in this case Franchise becomes equals to SubAxis
CASE WHEN l.FranchiseAgg IS NULL THEN q.classification.classificationSubAxis ELSE l.FranchiseAgg END as FranchiseAggregated,
CASE WHEN l.FranchiseAgg IS NULL THEN "OtherFranchise" ELSE l.FranchiseFilter END as FranchiseFiltering,
FROM (SELECT * FROM offline_orders UNION ALL SELECT * FROM online_orders) A
LEFT JOIN `emea-c1-dwh-prd.switzerland_all.item` q ON eanCode = q.variantInfo.eanCode
LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.FranchiseCH` l ON  (l.brand = q.classification.classificationSignature AND l.classificationSubBrand = q.classification.classificationSubBrand AND l.classificationAxis = q.classification.classificationAxis AND l.classificationSubAxis= q.classification.classificationSubAxis)
WHERE sourceChannel in ("online", "offline")
  and DATE(OrderDate) >= startDate
  and DATE(OrderDate) <= endDate
),

-- temp table to have product dimensions at ocdMasterID level  --- 
classification_ocdMasterID AS (
SELECT distinct
ocdMasterID,
classificationAxis as classificationAxis,
classificationSubAxis as classificationSubAxis,
classificationSubBrand as classificationSubBrand,
FranchiseAggregated as FranchiseAgg,
FranchiseFiltering as FranchiseFilter,
FROM omni_orders
GROUP BY 1,2,3,4,5,6
),

ordersKPI_tmp AS (
  SELECT
   ocdMasterID, 
  Count(distinct CASE WHEN EXTRACT(ISOYEAR FROM OrderDate) = EXTRACT(ISOYEAR FROM DATE(startDate)) 
  AND EXTRACT(MONTH FROM OrderDate) <= EXTRACT(MONTH FROM DATE(endDate)) THEN
  OrderId ELSE NULL END) AS Transactions_YTD_LastYear,
  Count(distinct CASE WHEN date(OrderDate)>=startDateL12M AND DATE(OrderDate)<=endDate THEN 
  OrderId ELSE NULL END) AS Transactions_L12M,
  Count(distinct CASE WHEN EXTRACT(ISOYEAR FROM OrderDate) = EXTRACT(ISOYEAR FROM DATE(endDate)) 
  AND EXTRACT(MONTH FROM OrderDate) <= EXTRACT(MONTH FROM DATE(endDate)) THEN 
  OrderId ELSE NULL END) AS Transactions_YTD_CurrentYear,    
  Count(distinct OrderId) AS Transactions_21and22,
  /*
  Count(distinct CASE WHEN EXTRACT(ISOYEAR FROM OrderDate) = EXTRACT(ISOYEAR FROM DATE(startDate)) AND EXTRACT(MONTH FROM OrderDate) <= EXTRACT(MONTH FROM DATE(endDate)) THEN ocdMasterID END) AS Customers_YTD_LastYear,
  Count(distinct CASE WHEN EXTRACT(ISOYEAR FROM OrderDate) = EXTRACT(ISOYEAR FROM DATE(endDate)) AND EXTRACT(MONTH FROM OrderDate) <= EXTRACT(MONTH FROM DATE(endDate)) THEN ocdMasterID END) AS Customers_YTD_CurrentYear,*/
  FROM omni_orders
  where sourceChannel in ("online", "offline")
  GROUP BY 1)

-- Table Consumer + all orders all grouped at consumer-related information level, not grouped at ocdMasterID --     
select
  contextMaster.brand,
  Case WHEN identityInfo.Gender.Gender="U" THEN "Unknown" ELSE identityInfo.Gender.Gender END as Gender,
  Case WHEN ageGroup is null THEN "Unknown" ELSE ageGroup END as ageGroup,
  Case WHEN ageGroup is null THEN "UnknownAgeGroup" ELSE "KnownAgeGroup" END as AgeGroupFilter,
  stores.affiliationChain,
  classification_ocdMasterID.classificationAxis,
  classification_ocdMasterID.classificationSubAxis,
  classification_ocdMasterID.FranchiseAgg,
  classification_ocdMasterID.FranchiseFilter,
  CASE 
    WHEN DATE(contactAcquisitionDate) > DATE_ADD(endDate,INTERVAL -180 DAY) then "<6M"
    WHEN DATE(contactAcquisitionDate) > DATE_ADD(endDate,INTERVAL -1 YEAR) then "06-12M"
    WHEN DATE(contactAcquisitionDate) > DATE_ADD(endDate,INTERVAL -2 YEAR) then "12-24M" 
    ELSE "24M+" end as RecencyRecruitment,
  CASE 
    WHEN DATE(lastValidPurchaseDate) > DATE_ADD(endDate,INTERVAL -180 DAY) then "<6M"
    WHEN DATE(lastValidPurchaseDate) > DATE_ADD(endDate,INTERVAL -1 YEAR) then "06-12M"
    WHEN DATE(lastValidPurchaseDate) > DATE_ADD(endDate,INTERVAL -2 YEAR) then "12-24M"
    WHEN lastValidPurchaseDate IS NULL then "prospect" ELSE "24M+" end as RecencyPurchase,
  CASE WHEN nbValidPurchase12M >= 4 THEN "4+" ELSE CAST(nbValidPurchase12M as string) end as FrequencyPurchaseL12M,
  CASE 
    WHEN validPurchaseAmount12M > 500 THEN "CHF 500+"
    WHEN validPurchaseAmount12M >= 300 THEN "300-500"
    WHEN validPurchaseAmount12M >= 150 THEN "150-300" 
    WHEN validPurchaseAmount12M > 0 THEN ">0-150" ELSE "0" end As MonetaryL12M,
  CASE 
    WHEN validPurchaseAmountTotal > 500 THEN "CHF 500+"
    WHEN validPurchaseAmountTotal >= 300 THEN "300-500"
    WHEN validPurchaseAmountTotal >= 150 THEN "150-300" 
    WHEN validPurchaseAmountTotal > 0 THEN ">0-150" ELSE "0" end As MonetaryLifetime,  
  CASE WHEN REGEXP_CONTAINS(preferences.language.language,"fr") THEN "fr" WHEN REGEXP_CONTAINS(preferences.language.language,"it") THEN "it" ELSE "de" END as Language, 
  CASE WHEN contactInfo.postal.zipCode IN ("1201", "1202", "1203", "1204", "1205", "1206", "1207", "1208", "1209", "1211", "1213", "1218", "1224", "1227", "1231", "1292") THEN "Geneve" 
    WHEN contactInfo.postal.zipCode IN ("8000", "8001", "8002", "8003", "8004", "8005", "8006", "8008", "8032", "8037", "8038", "8040", "8041", "8044", "8045", "8046", "8047",             "8048", "8049", "8050", "8051","8052", "8053", "8055", "8057", "8063", "8064", "8092", "8093", "8143") THEN "Zurich" 
    WHEN contactInfo.postal.zipCode IN ("1000", "1001", "1002", "1003", "1004", "1005", "1006", "1007", "1010", "1011", "1012", "1014", "1018", "1022", "1023", "1032", "1033",       "1052", "1053", "1068") THEN "Lausanne" 
    WHEN contactInfo.postal.zipCode IN ("4005", "4010", "4051", "4052", "4053", "4054", "4055", "4056", "4057", "4058", "4059") THEN "Bale" 
    WHEN contactInfo.postal.zipCode IN ("3001", "3004", "3005", "3006", "3007", "3008", "3010", "3011", "3012", "3013", "3014", "3015", "3018", "3019", "3020", "3027", "3032",       "3037", "3048", "3063", "3072", "3084", "3097", "3172") THEN "Berne" 
    WHEN contactInfo.postal.zipCode IN ("6003", "6004", "6005", "6006", "6014", "6015", "6016", "6020") THEN "Lucerne" 
    WHEN contactInfo.postal.zipCode IN ("6815", "6900", "6912", "6913", "6914", "6915", "6916", "6917", "6918", "6924", "6932", "6951", "6959", "6962", "6963", "6964", "6965",       "6966", "6967", "6968", "6974", "6976", "6977", "6978", "6979") THEN "Lugano"   
  ELSE "OtherCity" END as City,  
  sum(ordersKPI.Transactions_YTD_LastYear) as Transactions_YTD_LastYear,
  sum(ordersKPI.Transactions_YTD_CurrentYear) as Transactions_YTD_CurrentYear,
  sum(ordersKPI.Transactions_L12M) as Transactions_L12M,
  sum(ordersKPI.Transactions_21and22) as Transactions_21and22,

  
FROM `emea-c1-dwh-prd.switzerland_all.contact_master` contact
LEFT JOIN `ordersKPI_tmp` ordersKPI on ordersKPI.ocdMasterID = contact.ocdMasterID
LEFT JOIN `classification_ocdMasterID` classification_ocdMasterID on classification_ocdMasterID.ocdMasterId = contact.ocdMasterId
LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.StoreRefsCH` stores ON stores.affiliationStore = contact.preferences.affiliationStore.affiliationStore

WHERE contact.nominativeContact
  and division <> "Consumer Products Division"
  and contact.acquisitionSourcePlatform<> "customercare"
  and CHAR_LENGTH(contact.preferences.affiliationStore.affiliationStore)>2
  and REGEXP_CONTAINS(contextMaster.brand,brandRegex)
  and REGEXP_CONTAINS(classification_ocdMasterID.classificationAxis,"Fragrance|Hair|Hygiene|MakeUp|Skin Care")
  and nbValidPurchase > 0
  --and date(lastValidPurchaseDate) <= DATE_ADD(CURRENT_DATE(),Interval-852 DAY)

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

