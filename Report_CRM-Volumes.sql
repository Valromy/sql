DECLARE startDate DATE DEFAULT "2021-01-01" ;  
DECLARE endDate DATE DEFAULT "2023-01-01" ; 

-- first unnest owned products array to be able to define new metrics based on OP_Nature name    
WITH ownedproductTMP AS
(
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
GROUP BY 1,2),


-- define new metrics based on OP_Nature 
-- duplicate as necessary into 
Tmp_Query2 AS
(
SELECT
ocdContactMasterId,
SUM(CASE WHEN OP_Nature = 'Test_tell' THEN 1 ELSE 0 END) AS Nb_TestTell,
SUM(CASE WHEN OP_Nature = 'E-Sampling' THEN 1 ELSE 0 END) AS Nb_ESampling
FROM ownedproductTMP
GROUP BY 1
),

tracking_log_tmp AS (
  SELECT
    OCD_CONTACT_MASTER_ID as ocdMasterId,
    SUM(CASE WHEN DATE(TRACKINGLOG_DATE)>= startDate AND DATE(TRACKINGLOG_DATE)< endDate AND TRACKINGLOG_URL_TYPE="Open"
    THEN 1 ELSE 0 END) AS OpenedEmail,
    SUM(CASE WHEN DATE(TRACKINGLOG_DATE)>= startDate AND DATE(TRACKINGLOG_DATE)< endDate AND TRACKINGLOG_URL_TYPE="Email click"
    THEN 1 ELSE 0 END) AS ClickedEmail,
    SUM(CASE WHEN DATE(TRACKINGLOG_DATE)>= startDate AND DATE(TRACKINGLOG_DATE)< endDate AND TRACKINGLOG_URL_TYPE="Opt-out"
    THEN 1 ELSE 0 END) AS OptoutEmail,
  FROM `emea-c1-dwh-prd.switzerland_all.tracking_log`
  GROUP BY 1),
  
 broad_log_tmp AS (
  SELECT
    OCD_CONTACT_MASTER_ID as ocdMasterId,
    SUM(CASE WHEN DATE(BROADLOGRCP_CONTACT_DATE)>= startDate AND DATE(BROADLOGRCP_CONTACT_DATE)< endDate AND BROADLOGRCP_STATUS ="Sent" 
    THEN 1 ELSE 0 END) AS Targeted
  FROM `emea-c1-dwh-prd.switzerland_all.broad_log_rcp`
  GROUP BY 1),

Tmp_Query3 AS
(
SELECT
case when m.division = "Active Cosmetics Division" then "ACD"
when division = "Professional Products Division" then "PPD"
when division = "L'Oréal Luxe" then "Luxe"
else division end as division,  
case when m.contextMaster.brand = "LaRochePosay" then "LRP"
when m.contextMaster.brand = "GiorgioArmani" then "Armani"
when m.contextMaster.brand = "Ysl" then "YSL"
else m.contextMaster.brand end as brand,
m.contextMaster.country,
m.acquisitionSourcePlatform,
case WHEN REGEXP_CONTAINS(lower(contextMaster.brand), "larocheposay|vichy") AND REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"unknown|apotamox|neolane|demandware|wsf") AND
opR.RecruitmentOriginCampaign is not null then opR.RecruitmentOriginCampaign ELSE m.acquisitionSourceCampaignName END as acquisitionSourceCampaignName,
m.preferences.language.language,
m.identityInfo.gender.gender,
m.ageGroup,
preferences.affiliationStore.affiliationStore,
ocdContactMasterId,
CASE WHEN DATE(m.contactAcquisitionDate )< "2019-01-01" THEN "before2019" ELSE FORMAT_DATETIME("%Y", DATETIME (m.contactAcquisitionDate )) END as YearRecruitment,
    CASE
      WHEN REGEXP_CONTAINS(lower(contextMaster.brand), "larocheposay|vichy") THEN
      CASE WHEN Nb_ESampling >= 2 OR Nb_TestTell >= 2 and tracking.ClickedEmail>=2 THEN 1 ELSE 0 END
      WHEN REGEXP_CONTAINS(lower(contextMaster.brand), "giorgioarmani|kiehls|lancome|ysl") THEN
      CASE WHEN nbValidPurchase12M >=2 THEN 1 ELSE 0 END
      ELSE
      CASE WHEN tracking.ClickedEmail>=2 THEN 1 ELSE 0 END
      END AS Vip,
CASE WHEN DATE(m.contactAcquisitionDate) <  endDate THEN 1 ELSE 0 END AS Contacts,
CASE WHEN m.identityInfo.birthdate.birthDay IS NOT NULL THEN 1 ELSE 0 END AS FilledBirthdate,
CASE WHEN REGEXP_CONTAINS(m.identityInfo.Gender.Gender,"F|M") THEN 1 ELSE 0 END AS FilledGender,
-- CRM Targeting for email : (CRM6MActivity or refresh optin in past 180d or validpurchase in 12M) AND contactableEmail AND >=18 for known age AND not generic
CASE WHEN (DATE_DIFF(CURRENT_DATE(),EXTRACT(DATE FROM IF(m.lastOpenedEmail>m.contextMaster.creationdate,m.lastOpenedEmail,m.contextMaster.creationdate)), DAY) <= 183
OR m.validPurchaseAmount12M >0
OR DATE_DIFF(CURRENT_DATE(),EXTRACT(DATE FROM m.contactInfo.email.emailQualification.updateDate), DAY) <= 180)
AND m.isContactableEmail 
AND NOT m.genericContact
AND (m.birthday IS NULL OR m.realAge >= 18)
THEN 1 ELSE 0 END AS CRMActivity6M,

CASE WHEN m.prospect THEN 1 ELSE 0 END AS Prospect,
CASE WHEN m.directActiveCustomer THEN 1 ELSE 0 END AS ActiveCustomer,
CASE WHEN m.directInactiveCustomer THEN 1 ELSE 0 END AS InactiveCustomer,
CASE WHEN m.isContactable THEN 1 ELSE 0 END AS Contactable,
CASE WHEN m.isContactableEmail THEN 1 ELSE 0 END AS ContactableEmail,
CASE WHEN m.isContactablePostal THEN 1 ELSE 0 END AS ContactablePost,
CASE WHEN m.isContactableSms THEN 1 ELSE 0 END AS ContactableSMS,
CASE WHEN m.isContactableMobilePhone THEN 1 ELSE 0 END AS ContactableMobilePhone,
CASE WHEN m.mobileNumber is not null THEN 1 ELSE 0 END AS KnownMobileNumber,

CASE WHEN m.engagedContact THEN 1 ELSE 0 END AS isEngagedContact,

CASE WHEN broadlog.Targeted>=1 THEN 1 ELSE 0 END as EmailReceivers,
CASE WHEN tracking.OpenedEmail>=1 THEN 1 ELSE 0 END as Openers,
CASE WHEN tracking.ClickedEmail>=1 THEN 1 ELSE 0 END as Clickers,
CASE WHEN tracking.OptoutEmail>=1 THEN 1 ELSE 0 END as OptoutEmail,

FROM `emea-c1-dwh-prd.switzerland_all.contact_master` m
LEFT JOIN Tmp_Query2 q ON m.contextMaster.ocdMasterId = q.ocdContactMasterId
LEFT JOIN tracking_log_tmp tracking ON m.contextMaster.ocdMasterId = tracking.ocdMasterId
LEFT JOIN broad_log_tmp broadlog ON m.contextMaster.ocdMasterId = broadlog.ocdMasterId
LEFT JOIN opRecruitmentCampaign opR ON m.contextMaster.ocdMasterId = opR.ocdMasterID

-- exclude CC care contacts
WHERE not m.genericContact
and not m.anonymizedContact
and not m.cCareNonOptinContact
AND DATE(m.contactAcquisitionDate) <  endDate
and division <> "Consumer Products Division"
and acquisitionSourcePlatform<> "customercare"
and not regexp_contains(lower(m.contextMaster.brand), "valentinoeu|valentino eu|helenarubinsteineu|giorgioarmanieu|urbandecay|ysleu"))

-- Final count by contact_master
SELECT
division,
brand,
YearRecruitment,
case WHEN gender="U" THEN "Unknown" ELSE gender END as Gender,
case WHEN ageGroup is null THEN "Unknown" ELSE ageGroup END as ageGroup,
CASE 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"unknown|apotamox|neolane|demandware|wsf") THEN "Website" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"mars|localsourcesystem|pos|btr") THEN "Offline - Mars" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"qualifio") THEN "Media - Qualifio" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"facebook") THEN "Media - Facebook"
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"sampler") THEN "Media - Sampler" else acquisitionSourcePlatform END
as RecruitmentSource, 
acquisitionSourceCampaignName,
CASE WHEN Vip=1 THEN "VIP" ELSE "Commoner" END as Vip_YN,
CASE WHEN REGEXP_CONTAINS(language,"fr") THEN "fr" WHEN REGEXP_CONTAINS(language,"it") THEN "it" ELSE "de" END as Language, 
s.ProperAffiliationStore,
s.affiliationChain,
Case when division <> "L'Oréal Luxe" THEN "NotAffiliated"
when Tmp_Query3.affiliationStore is NULL THEN "NotAffiliated" Else "Affiliated" End as CheckAffiliation, 
SUM(Contacts) AS nbContacts,
SUM(isEngagedContact) AS EngagedContacts,
SUM(Vip) AS Vip,
SUM(FilledBirthdate) AS FilledBirthdate,
SUM(FilledGender) AS FilledGender,
SUM(Prospect) AS Prospect,
SUM(ActiveCustomer) AS ActiveCustomer,
SUM(InactiveCustomer) AS InactiveCustomer,
SUM(Contactable) AS Contactable,
SUM(ContactableEmail) AS ContactableEmail,
SUM(ContactablePost) AS ContactablePost,
SUM(KnownMobileNumber) AS KnownMobileNumber,
SUM(ContactableMobilePhone) AS ContactableMobilePhone,
SUM(ContactableSMS) AS ContactableSMS,
SUM(CRMActivity6M) AS EmailTargeting,
SUM(Openers) as EmailOpenersYTD,
SUM(EmailReceivers) as EmailReceiversYTD,
SUM(Clickers) as EmailValuableClickersYTD,
SUM(CASE WHEN Vip=1 AND CRMActivity6M=1 THEN 1 ELSE 0 END) as EmailTargeting_Vip,
SUM(CASE WHEN FilledBirthdate=1 AND CRMActivity6M=1 THEN 1 ELSE 0 END) as EmailTargeting_knownBirthday,
SUM(CASE WHEN FilledGender=1 AND CRMActivity6M=1 THEN 1 ELSE 0 END) as EmailTargeting_knownGender,
SUM(CASE WHEN Prospect=1 AND CRMActivity6M=1 THEN 1 ELSE 0 END) as EmailTargeting_Prospect,
SUM(CASE WHEN ActiveCustomer=1 AND CRMActivity6M=1 THEN 1 ELSE 0 END) as EmailTargeting_ActiveCustomer,
SUM(CASE WHEN InactiveCustomer=1 AND CRMActivity6M=1 THEN 1 ELSE 0 END) as EmailTargeting_InactiveCustomer,


FROM Tmp_Query3
LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.StoreRefsCH` s ON s.affiliationStore = Tmp_Query3.affiliationStore


--and division <> "L'Oréal Luxe"

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY 1 , 2
