DECLARE brandRegex STRING DEFAULT "Lancome|Kiehls|Ysl|GiorgioArmani";
DECLARE startDate DATE DEFAULT "2019-01-01" ;
DECLARE endDate DATE DEFAULT "2023-01-01" ;


WITH offline_orders AS (
SELECT
  a.context.ocdTicketId AS OrderId,
  a.header.ocdContactMasterId AS ocdMasterId,
  source.sourceBaId,
  eanCode,
  a.header.ticketDate AS OrderDate,
  LOWER(source.sourceChannel) AS sourceChannel,
  a.header.purchaseAmountTaxIncludedAfterDiscount as CHF,
  SUM(itemQuantity) as UN
FROM `emea-c1-dwh-prd.switzerland_all.sales_history` a, UNNEST(lines) as hits 
WHERE a.validPurchase and REGEXP_CONTAINS(a.context.brand, brandRegex) and lineAmountTaxIncludedAfterDiscount	> 0
GROUP BY 1,2,3,4,5,6,7
),

-- rank orderID by orderDate at ocdMasterID level
dedup_omni_orders AS (SELECT Distinct ocdMasterID, OrderId, OrderDate, sourceBaId FROM offline_orders),
OrderNumberDB AS (SELECT ocdMasterID, OrderId, OrderDate,sourceBaId, ROW_NUMBER() OVER (PARTITION BY ocdMasterID ORDER BY OrderDate ASC ) AS OrderRank
FROM dedup_omni_orders),

ContactabilityAtRecruitment as (
SELECT distinct 
  context.ocdMasterId,
  cv.consentInfo.optinEmail.optinEmail,
  cv.contactInfoDqm.emailDqm.emailQualification.emailValidity,
  cv.consentInfo.optinSms.optinSms,
  cv.contactInfoDqm.mobileDqm.mobileQualification.mobileValidity,
  cv.consentInfo.optinPostal.optinPostal,
  cv.contactInfoDqm.postalDqm.postalQualification.postalValidity
FROM `emea-c1-dwh-prd.switzerland_all.contact_versioning` cv
INNER JOIN `emea-c1-dwh-prd.switzerland_all.contact_master`cm
ON cm.contextMaster.ocdMasterId = cv.context.ocdMasterId AND cm.contactAcquisitionDate = cv.context.creationDate),

ocdMasterIDwithBA as (
SELECT 
  ocdMasterID,
  sourceBaId
FROM OrderNumberDB
Where OrderRank=1
GROUP BY 1,2)

SELECT
  lower(contextMaster.brand) as brand,
--  CASE WHEN DATE(contextMaster.creationDate)< "2019-01-01" THEN "before2019" ELSE FORMAT_DATETIME("%y %m", DATETIME (contextMaster.creationDate)) END as YYMM,
  CASE WHEN DATE(contactAcquisitionDate)< "2019-01-01" THEN "before2019" ELSE FORMAT_DATETIME("%Y", DATETIME (contactAcquisitionDate)) END as Year,
    CASE WHEN DATE(contactAcquisitionDate)< "2021-01-01" THEN null ELSE FORMAT_DATETIME("%m", DATETIME (contactAcquisitionDate)) END as Month,
  CASE WHEN (s.ProperAffiliationStore IS NULL AND acquisitionSourceChannel="online") THEN "online" 
       WHEN s.ProperAffiliationStore IS NULL THEN base.preferences.affiliationStore.affiliationStore 
  ELSE  s.ProperAffiliationStore END as AffiliationStore,
  CASE WHEN (s.affiliationChain IS NULL AND acquisitionSourceChannel="online") THEN "online" ELSE s.affiliationChain END as AffiliationChain,
  case WHEN ba.BaName IS NULL THEN "GENERIC" ELSE ba.BaName END as BaName,
  Case WHEN ba.BaName ="GENERIC" or ba.BaName IS NULL THEN "Generic BA" ELSE "Nominative BA" END BaFilter,
  Count(distinct contextMaster.ocdMasterId ) AS nbContacts,
  Count(distinct (case when consentInfo.optinEmail.optinEmail = "Yes" then contextMaster.ocdMasterId end)) as optinEmail,
  Count(distinct (case when c.optinEmail="Yes" then contextMaster.ocdMasterId end)) as OptinEmail_R,
  Count(distinct (case when c.emailValidity = "OK" then contextMaster.ocdMasterId end)) as EmailValidity_R,
  Count(distinct (case when isContactableEmail then contextMaster.ocdMasterId end)) as ContactableEmail,
  Count(distinct (case when c.optinSMS="Yes" then contextMaster.ocdMasterId end)) as OptinSMS_R,
  Count(distinct (case when c.mobileValidity="OK" then contextMaster.ocdMasterId end)) as ValidtySMS_R,
  Count(distinct (case when isContactableSms then contextMaster.ocdMasterId end)) as ContactableSMS,
  Count(distinct (case when c.optinPostal="Yes" then contextMaster.ocdMasterId end)) as OptinPostal_R,
  Count(distinct (case when c.postalValidity="OK" then contextMaster.ocdMasterId end)) as PostalValidity_R,
  Count(distinct (case when isContactablePostal then contextMaster.ocdMasterId end)) as ContactablePostal,
  /*Count(distinct (CASE WHEN (DATE_DIFF(CURRENT_DATE(),EXTRACT(DATE FROM IF(lastOpenedEmail>contextMaster.creationdate,lastOpenedEmail,contextMaster.creationdate)), DAY) <= 183
    OR validPurchaseAmount12M >0
    OR DATE_DIFF(CURRENT_DATE(),EXTRACT(DATE FROM contactInfo.email.emailQualification.updateDate), DAY) <= 180)
    AND isContactableEmail 
    AND NOT genericContact
    AND (birthday IS NULL OR realAge >= 18)
    THEN base.contextMaster.ocdMasterId END)) as CRMActivity6M,
  Count(distinct (case when newActiveCustomer then base.contextMaster.ocdMasterId end)) as nbNewCustomers*/

FROM `switzerland_all.contact_master` base
LEFT JOIN ocdMasterIDwithBA b ON base.contextMaster.ocdMasterId=b.ocdMasterID
LEFT JOIN ContactabilityAtRecruitment c ON base.contextMaster.ocdMasterId= c.ocdMasterID 
LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.StoreRefsCH` s ON s.affiliationStore = base.preferences.affiliationStore.affiliationStore
LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.BAsCH` ba ON ba.BA_ID = b.sourceBaId

WHERE
    contextMaster.country = "CH"
    and REGEXP_CONTAINS(contextMaster.brand, brandRegex)
    and not genericContact
    and not anonymizedContact
    and not cCareNonOptinContact
    and nominativeContact
    and acquisitionSourcePlatform<> "customercare"
    and DATE(contactAcquisitionDate)>= startDate
    and DATE(contactAcquisitionDate)< endDate
   
GROUP BY 1,2,3,4,5,6,7