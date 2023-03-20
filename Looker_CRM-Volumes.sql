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


Tmp_Query3 AS
(
SELECT
case when m.division = "Active Cosmetics Division" then "ACD"
when division = "Professional Products Division" then "PPD"
when division = "L'Oréal Luxe" then "Luxe"
else division end as division,  
case when m.contextMaster.brand = "LaRochePosay" then "LRP"
when m.contextMaster.brand = "Kerastase" then "Kérastase"
when m.contextMaster.brand = "Lancome" then "Lancôme"
when m.contextMaster.brand = "Kiehls" then "Kiehl's"
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
CASE WHEN (s.ProperAffiliationStore IS NULL AND acquisitionSourceChannel="online") THEN "online" 
       WHEN s.ProperAffiliationStore IS NULL THEN preferences.affiliationStore.affiliationStore 
  ELSE  s.ProperAffiliationStore END as AffiliationStore,
  CASE WHEN (s.affiliationChain IS NULL AND acquisitionSourceChannel="online") THEN "online" ELSE s.affiliationChain END as AffiliationChain,
ocdContactMasterId,
CASE WHEN DATE(m.contactAcquisitionDate )< "2019-01-01" THEN "before2019" ELSE FORMAT_DATETIME("%Y", DATETIME (m.contactAcquisitionDate )) END as YearRecruitment,
CASE WHEN m.nominativeContact THEN 1 ELSE 0 END AS Contacts,
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

FROM `emea-c1-dwh-prd.switzerland_all.contact_master` m
LEFT JOIN Tmp_Query2 q ON m.contextMaster.ocdMasterId = q.ocdContactMasterId

LEFT JOIN opRecruitmentCampaign opR ON m.contextMaster.ocdMasterId = opR.ocdMasterID
LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.StoreRefsCH` s ON s.affiliationStore = preferences.affiliationStore.affiliationStore

-- exclude CC care contacts
WHERE not m.genericContact
and not m.anonymizedContact
and not m.cCareNonOptinContact
and division <> "Consumer Products Division"
and not regexp_contains(lower(m.contextMaster.brand), "valentinoeu|valentino eu|helenarubinsteineu|giorgioarmanieu|urbandecay|ysleu")
and acquisitionSourcePlatform<> "customercare"
)

-- Final count by contact_master
SELECT
division,
brand,
YearRecruitment,
--case WHEN gender="U" THEN "Unknown" ELSE gender END as Gender,
--case WHEN ageGroup is null THEN "Unknown" ELSE ageGroup END as ageGroup,
CASE 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"unknown|apotamox|neolane|demandware|wsf") THEN "Website" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"mars|localsourcesystem|pos|btr") THEN "Offline - Mars" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"qualifio") THEN "Media - Qualifio" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"facebook") THEN "Media - Facebook"
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"sampler") THEN "Media - Sampler" else acquisitionSourcePlatform END
as RecruitmentSource, 
acquisitionSourceCampaignName,
--CASE WHEN REGEXP_CONTAINS(language,"fr") THEN "fr" WHEN REGEXP_CONTAINS(language,"it") THEN "it" ELSE "de" END as Language, 
affiliationStore,
affiliationChain,
SUM(Contacts) AS Contacts,
SUM(isEngagedContact) AS EngagedContacts,
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
SUM(CASE WHEN FilledBirthdate=1 AND CRMActivity6M=1 THEN 1 ELSE 0 END) as EmailTargeting_knownBirthday,
SUM(CASE WHEN FilledGender=1 AND CRMActivity6M=1 THEN 1 ELSE 0 END) as EmailTargeting_knownGender,
SUM(CASE WHEN Prospect=1 AND CRMActivity6M=1 THEN 1 ELSE 0 END) as EmailTargeting_Prospect,

FROM Tmp_Query3
GROUP BY 1,2,3,4,5,6,7