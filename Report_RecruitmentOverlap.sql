DECLARE startDate DATE DEFAULT "2021-01-01" ;  
DECLARE endDate DATE DEFAULT "2022-01-01" ; 

-- first unnest owned products array to be able to define new metrics based on OP_Nature name    
with ownedproductTMP AS(
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
  
col1 as (SELECT distinct
contextMaster.brand, 
contextMaster.ocdMasterId,
lower(contactInfo.email.emailAddress) as email
FROM `emea-c1-dwh-prd.switzerland_all.contact_master`
where contactInfo.email.emailAddress is not null
and length(contactInfo.email.emailAddress)>3
and not genericContact and not anonymizedContact and not cCareNonOptinContact and acquisitionSourcePlatform<> "customercare"
AND DATE(contactAcquisitionDate) <  endDate and division <> "Consumer Products Division" and contextMaster.ocdMasterId is not null
group by 1,2,3),

col2 as (SELECT distinct
contextMaster.brand, 
contextMaster.ocdMasterId,
lower(contactInfo.email.emailAddress) as email
FROM `emea-c1-dwh-prd.switzerland_all.contact_master`
where contactInfo.email.emailAddress is not null and contextMaster.brand is not null
and length(contactInfo.email.emailAddress)>3
and not genericContact and not anonymizedContact and not cCareNonOptinContact and acquisitionSourcePlatform<> "customercare"
AND DATE(contactAcquisitionDate) <  endDate and division <> "Consumer Products Division" and contextMaster.ocdMasterId is not null
group by 1,2,3),

colIndex as (Select
distinct col1.ocdMasterId,
col2.brand as BrandColumnforOverlap,
from col1 as col1
LEFT JOIN col2 col2 on col1.email = col2.email
where col2.brand is not null
group by 1,2),

Tmp_Query3 AS
(SELECT
m.contextMaster.ocdMasterId,
m.contextMaster.brand,
c.BrandColumnforOverlap,
CASE 
  WHEN REGEXP_CONTAINS(lower(m.acquisitionSourcePlatform),"unknown|apotamox|neolane|demandware|wsf") THEN "Website" 
  WHEN REGEXP_CONTAINS(lower(m.acquisitionSourcePlatform),"mars|localsourcesystem|pos|btr") THEN "Offline - Mars" 
  WHEN REGEXP_CONTAINS(lower(m.acquisitionSourcePlatform),"qualifio") THEN "Media - Qualifio" 
  WHEN REGEXP_CONTAINS(lower(m.acquisitionSourcePlatform),"facebook") THEN "Media - Facebook"
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"sampler") THEN "Media - Sampler" else m.acquisitionSourcePlatform END
as RecruitmentSource, 
case WHEN REGEXP_CONTAINS(lower(contextMaster.brand), "larocheposay|vichy") AND REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"unknown|apotamox|neolane|demandware|wsf") AND
opR.RecruitmentOriginCampaign is not null then opR.RecruitmentOriginCampaign ELSE m.acquisitionSourceCampaignName END as acquisitionSourceCampaignName,
m.preferences.language.language,
m.identityInfo.gender.gender,
m.ageGroup,
preferences.affiliationStore.affiliationStore,
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
CASE WHEN m.birthday IS NOT NULL THEN 1 ELSE 0 END AS FilledBirthdate,
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
LEFT JOIN colIndex c ON m.contextMaster.ocdMasterId = c.ocdMasterId
-- exclude CC care contacts
WHERE not m.genericContact
and not m.anonymizedContact
and not m.cCareNonOptinContact
AND DATE(m.contactAcquisitionDate) <  endDate
and division <> "Consumer Products Division"
and m.acquisitionSourcePlatform<> "customercare"
--and ocdContactMasterId is not null
),

-- need to produce allpossible combinations of brand, recruitmentSource brandcolumn overlap, RecruitmentSourceOverlap, YearRecruitment
-- Final count by contact_master
ante as(SELECT
CONCAT(brand,YearRecruitment,RecruitmentSource) as keyMax,
CONCAT(brand,BrandColumnforOverlap,YearRecruitment,RecruitmentSource) as keyTable,
brand,
BrandColumnforOverlap,
YearRecruitment,
RecruitmentSource,
SUM(Contacts) AS nbContacts,
SUM(CRMActivity6M) AS EmailTargeting,
SUM(Clickers) as EmailValuableClickersYTD,
Case when brand=BrandColumnforOverlap then SUM(Contacts) end as denContacts,
Case when brand=BrandColumnforOverlap then SUM(CRMActivity6M) end as denCRMActivity6M,
Case when brand=BrandColumnforOverlap then SUM(Clickers) end as denClickers,
FROM Tmp_Query3
where BrandColumnforOverlap is not null --and RecruitmentSourceOverlap is not null
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6),

localdenContacts as (select keyMax, 
denContacts FROM(select keyMax, denContacts, ROW_NUMBER() OVER (PARTITION BY keyMax ORDER BY denContacts DESC ) AS rn from ante) where rn =1),
localdenCRMActivity6M as (select keyMax, 
denCRMActivity6M FROM(select keyMax, denCRMActivity6M, ROW_NUMBER() OVER (PARTITION BY keyMax ORDER BY denCRMActivity6M DESC ) AS rn from ante) where rn =1),
localdenClickers as (select keyMax, 
denClickers FROM(select keyMax, denClickers, ROW_NUMBER() OVER (PARTITION BY keyMax ORDER BY denClickers DESC ) AS rn from ante) where rn =1),

crossTable as (Select distinct
CONCAT(t1.brand,t2.YearRecruitment, t2.RecruitmentSource) as keyMax,
CONCAT(t1.brand, t1.BrandColumnforOverlap, t2.YearRecruitment,  t2.RecruitmentSource) as keyTable,
t1.brand, t1.BrandColumnforOverlap , t2.YearRecruitment, t2.RecruitmentSource
FROM ante t1
CROSS JOIN ante t2
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6)

Select
case when regexp_contains(c.brand, "Biotherm|GiorgioArmani|GiorgioArmaniEU|HelenaRubinsteinEU|Kiehls|Lancome|Mugler|UrbanDecay|ValentinoEU|Ysl") then "LLD" when regexp_contains(c.brand, "CeraVe|LaRochePosay|SkinCeuticals|Vichy") then "ACD" when c.brand = "Kerastase" then "PPD" else c.brand end as division,
case when regexp_contains(c.BrandColumnForOverlap, "Biotherm|GiorgioArmani|GiorgioArmaniEU|HelenaRubinsteinEU|Kiehls|Lancome|Mugler|UrbanDecay|ValentinoEU|Ysl") then "LLD" when regexp_contains(c.BrandColumnForOverlap, "CeraVe|LaRochePosay|SkinCeuticals|Vichy") then "ACD" when c.BrandColumnForOverlap = "Kerastase" then "PPD" else c.BrandColumnForOverlap end as divisionOverlap,
c.brand,
c.BrandColumnForOverlap, 
c.YearRecruitment, 
c.RecruitmentSource, 
a.nbContacts,
a.EmailTargeting,
a.EmailValuableClickersYTD,
l.denContacts,
lCRMA.denCRMActivity6M,
lClick.denClickers
from crosstable c
LEFT JOIN localdenContacts l on l.keyMax = c.keyMax
LEFT JOIN localdenCRMActivity6M lCRMA on lCRMA.keyMax = c.keyMax
LEFT JOIN localdenClickers lCLick on lCLick.keyMax = c.keyMax
LEFT JOIN ante a on a.keyTable = c.keyTable
where a.nbContacts is not null 
or a.EmailTargeting is not null 
or a.EmailValuableClickersYTD is not null 
or l.denContacts is not null 
or lCRMA.denCRMActivity6M is not null 
or lClick.denClickers is not null
order by 1,2,3,4,5,6,7

