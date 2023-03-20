DECLARE endDate DATE DEFAULT "2023-01-01" ; 

SELECT
  contextMaster.brand,
  CASE WHEN (s.ProperAffiliationStore IS NULL AND acquisitionSourceChannel="online") THEN "online" 
       WHEN s.ProperAffiliationStore IS NULL THEN preferences.affiliationStore.affiliationStore 
  ELSE  s.ProperAffiliationStore END as AffiliationStore,
  CASE WHEN (s.affiliationChain IS NULL AND acquisitionSourceChannel="online") THEN "online" ELSE s.affiliationChain END as AffiliationChain,
  --ocdMasterID,
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
  
  Count(distinct contextMaster.ocdMasterId ) AS nbContacts,  
  Count(distinct (case when isContactableEmail then contextMaster.ocdMasterId end)) as ContactableEmail,   
  Count(distinct (case when isContactableSms then contextMaster.ocdMasterId end)) as ContactableSMS,   
  Count(distinct (CASE WHEN (DATE_DIFF(CURRENT_DATE(),EXTRACT(DATE FROM IF(lastOpenedEmail>contextMaster.creationdate,lastOpenedEmail,contextMaster.creationdate)), DAY) <= 183
    OR validPurchaseAmount12M >0
    OR DATE_DIFF(CURRENT_DATE(),EXTRACT(DATE FROM contactInfo.email.emailQualification.updateDate), DAY) <= 180)
    AND isContactableEmail 
    AND NOT genericContact
    AND (birthday IS NULL OR realAge >= 18)
    THEN contextMaster.ocdMasterId END)) as EmailTargeting,
  SUM(validPurchaseAmount12M) as OrdersCHF12M,
  SUM(nbValidPurchase12M) as Orders12M,
  Count(distinct case when nbValidPurchase12M>0 then contextMaster.ocdMasterId end) as Buyers12M,


FROM `switzerland_all.contact_master`
LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.StoreRefsCH` s ON s.affiliationStore = preferences.affiliationStore.affiliationStore

WHERE 
    contextMaster.country = "CH"
    and REGEXP_CONTAINS(contextMaster.brand, "Lancome|Kiehls|Ysl|GiorgioArmani|Helena|Valentino")
    and not genericContact
    and not anonymizedContact
    and not cCareNonOptinContact
    and nominativeContact
    and acquisitionSourcePlatform<> "customercare"   
    
 GROUP BY 1,2,3,4,5,6,7,8