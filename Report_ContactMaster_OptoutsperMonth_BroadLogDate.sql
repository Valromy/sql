DECLARE TimeRegex STRING DEFAULT "20(19|20|21|22|23)_..";

WITH tracking_log_tmp AS (
  SELECT
    OCD_CONTACT_MASTER_ID as ocdMasterId,
    FORMAT_DATETIME("%Y_%m", DATETIME (TRACKINGLOG_DATE)) as NLContactDate_YYYY_mm,
    SUM(CASE WHEN TRACKINGLOG_URL_TYPE="Open" THEN 1 ELSE 0 END) AS OpenedEmail,
    SUM(CASE WHEN TRACKINGLOG_URL_TYPE="Email click" THEN 1 ELSE 0 END) AS ClickedEmail,
    SUM(CASE WHEN TRACKINGLOG_URL_TYPE="Opt-out" THEN 1 ELSE 0 END) AS OptoutEmail,
  FROM `emea-c1-dwh-prd.switzerland_all.tracking_log`
  WHERE REGEXP_CONTAINS(FORMAT_DATETIME("%Y_%m", DATETIME (TRACKINGLOG_DATE)),TimeRegex)
  GROUP BY 1,2 ORDER BY 1),

  broad_log_tmp AS (
  SELECT
    OCD_CONTACT_MASTER_ID as ocdMasterId,
    FORMAT_DATETIME("%Y_%m", DATETIME (BROADLOGRCP_CONTACT_DATE)) as NLContactDate_YYYY_mm,
    SUM(CASE WHEN BROADLOGRCP_STATUS ="Sent" THEN 1 ELSE 0 END) AS Targeted
  FROM `emea-c1-dwh-prd.switzerland_all.broad_log_rcp`
  WHERE REGEXP_CONTAINS(FORMAT_DATETIME("%Y_%m", DATETIME (BROADLOGRCP_CONTACT_DATE)),TimeRegex)
  GROUP BY 1,2 ORDER BY 1),
  
  MonthlyOptouts_tmp AS (
  SELECT
  broad.ocdMasterId,
  broad.NLContactDate_YYYY_mm,
  SUM(broad.Targeted) as Targeted,
  SUM(tracking.OpenedEmail) as OpenedEmail,
  SUM(tracking.ClickedEmail) as ClickedEmail,
  SUM(tracking.OptoutEmail) as OptoutEmail,
  FROM broad_log_tmp broad
  LEFT JOIN tracking_log_tmp tracking on (tracking.ocdMasterId = broad.ocdMasterId AND tracking.NLContactDate_YYYY_mm = broad.NLContactDate_YYYY_mm)
  GROUP BY 1,2),
  
  ownedproductTMP AS (
  SELECT
  context.brand,
  int.interaction.ocdContactMasterId,
  int.context.ocdInteractionId,
  interaction.interactionDate,
  context.creationDate,
  source.sourceName,
  source.sourceChannel,
  int.interaction.actionType,
  MAX(IF(attributes.attributeType = "Nature", attributes.attributeValue, NULL)) AS OP_Nature,
  MAX(IF(attributes.attributeType = "OperationName", attributes.attributeValue, NULL)) AS OP_Operation,
  MAX(IF(attributes.attributeType = "EANCode", attributes.attributeValue, NULL)) AS OP_EAN,
  FROM
  `switzerland_all.interaction` int,
  UNNEST( int.interaction.attributes) attributes
  WHERE REGEXP_CONTAINS(int.interaction.actionType, "Own_Product|Own_Sample|Test_Product")
  GROUP BY 1,2,3,4,5,6,7,8
  ),

  dedup_op AS (SELECT Distinct ocdContactMasterId, ocdInteractionId, OP_Nature, OP_Operation, interactionDate FROM ownedproductTMP),
  opbooldb AS (
  SELECT d.ocdContactMasterId as ocdMasterID, ocdInteractionId, interactionDate,
  ROW_NUMBER() OVER (PARTITION BY d.ocdContactMasterId ORDER BY interactionDate ASC ) AS OPRank,
  CASE WHEN DATE_DIFF(DATE(interactionDate), DATE(m.contactAcquisitionDate), DAY)=0 THEN 1 ELSE 0 END as RecruitedFromOP,
  op.RecruitmentOriginCampaign
  FROM dedup_op d
  LEFT JOIN `switzerland_all.contact_master` m on m.contextMaster.ocdMasterId = d.ocdContactMasterId
  LEFT JOIN `switzerland_all_local.OwnedProductCH` op on op.OP_Nature = d.OP_Nature and op.OP_Operation = d.OP_Operation),

  opRecruitmentCampaign as (
  SELECT distinct ocdMasterID, RecruitmentOriginCampaign
  FROM opbooldb where OPRank=1 and RecruitedFromOP=1
  GROUP BY 1,2)
  
  SELECT
division,
contextMaster.brand,
case WHEN REGEXP_CONTAINS(lower(contextMaster.brand), "larocheposay|vichy") AND REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"unknown|apotamox|neolane|demandware|wsf") AND
opR.RecruitmentOriginCampaign is not null then opR.RecruitmentOriginCampaign ELSE m.acquisitionSourceCampaignName END as acquisitionSourceCampaignName,
CASE 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"unknown|apotamox|neolane|demandware|wsf") THEN "Website" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"mars|localsourcesystem|pos|btr") THEN "Offline - Mars" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"qualifio") THEN "Media - Qualifio" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"facebook") THEN "Media - Facebook" END
as RecruitmentSource, 
CASE WHEN REGEXP_CONTAINS(preferences.language.language,"fr") THEN "fr" WHEN REGEXP_CONTAINS(preferences.language.language,"it") THEN "it" ELSE "de" END as Language, 
/*identityInfo.gender.gender,
ageGroup,
s.ProperAffiliationStore,
s.affiliationChain,
CASE WHEN division <> "L'Oréal Luxe" THEN "NotAffiliated"
WHEN preferences.affiliationStore.affiliationStore is NULL THEN "NotAffiliated" ELSE "Affiliated" END as CheckAffiliation, */ 
MonthlyOptouts.NLContactDate_YYYY_mm as EmailContactMonth,

SUM(CASE WHEN MonthlyOptouts.Targeted>=1 THEN 1 ELSE 0 END) as EmailReceivers,
SUM(CASE WHEN MonthlyOptouts.OpenedEmail>=1 THEN 1 ELSE 0 END) as Openers,
SUM(CASE WHEN MonthlyOptouts.ClickedEmail>=1 THEN 1 ELSE 0 END) as Clickers,
SUM(CASE WHEN MonthlyOptouts.OptoutEmail>=1 THEN 1 ELSE 0 END) as OptoutEmail,

FROM `emea-c1-dwh-prd.switzerland_all.contact_master` m
INNER JOIN MonthlyOptouts_tmp MonthlyOptouts ON m.contextMaster.ocdMasterId = MonthlyOptouts.ocdMasterId
LEFT JOIN opRecruitmentCampaign opR ON m.contextMaster.ocdMasterId = opR.ocdMasterID
--LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.StoreRefsCH` s ON s.affiliationStore = m.preferences.affiliationStore.affiliationStore

-- exclude CC care contacts
WHERE not genericContact
AND not anonymizedContact
AND not cCareNonOptinContact
AND division <> "Consumer Products Division"
AND acquisitionSourcePlatform<> "customercare"
AND REGEXP_CONTAINS(MonthlyOptouts.NLContactDate_YYYY_mm,TimeRegex)

GROUP BY 1,2,3,4,5,6
ORDER BY 1, 2 , 3



