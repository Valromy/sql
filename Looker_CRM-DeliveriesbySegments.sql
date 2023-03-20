WITH localNaming as (SELECT 
  d.DELIVERY_LABEL as DeliveryLabel,
  DELIVERY_ID_AC as DeliveryID,
  DELIVERY_SUBJECT as Subject,
  case  when n.triggerType is NULL then d.DELIVERY_LABEL	else
  d.BRAND||n.triggerType||n.triggerName||n.locale end as keyTrigger,
  DELIVERY_CONTACT_DATE as DeliveryDate,
FROM
  `emea-c1-dwh-prd.switzerland_all.delivery` d
  LEFT JOIN  `emea-c1-dwh-prd.switzerland_all_local.triggerNamingCH` n on n.DeliveryLabel = d.DELIVERY_LABEL
WHERE
  DELIVERY_CHANNEL ="Email"
  and DELIVERY_CONTACT_DATE	is not null
  and d.CAMPAIGN_ID_AC <> "0"
  and cast(DELIVERY_SUCCESS as int64) >0
  and TYPE__PC = "Trigger"),

helpPreview as (select keyTrigger, DeliveryIDPreview, lastSubject
from (select DeliveryID as DeliveryIDPreview,Subject as lastSubject, keyTrigger,DeliveryDate, ROW_NUMBER() OVER (PARTITION BY keyTrigger ORDER BY DeliveryDate DESC ) AS DeliveryRank ,
FROM localNaming where keyTrigger is not null) where DeliveryRank =1),

indexPreview as (select n.DeliveryID, h.DeliveryIDPreview, h.lastSubject
from localNaming n
LEFT JOIN helpPreview h on h.keyTrigger = n.keyTrigger),

 deliveryBetter as (SELECT
  case when regexp_contains(d.BRAND,"YSL|Biotherm|Helena Rubinstein EU|Mugler|Giorgio Armani|Giorgio Armani EU|Lancôme|Urban Decay|Valentino EU|ValentinoEU|Kiehl's") then "Luxe"
  when regexp_contains(d.BRAND,"CeraVe|Derma Center|La Roche Posay|Skinceuticals|Vichy") then "ACD"
  when d.BRAND ="Kerastase" then "PPD" end as division, 
  d.BRAND as Brand,
  EXTRACT(YEAR FROM DELIVERY_CONTACT_DATE) as Year,
  EXTRACT(HOUR FROM DELIVERY_CONTACT_DATE) as HourExact,
  EXTRACT(TIME FROM DELIVERY_CONTACT_DATE) as Time,
  EXTRACT(DAYOFWEEK FROM DELIVERY_CONTACT_DATE) as DayOfWeek,  
  EXTRACT(DATE FROM DELIVERY_CONTACT_DATE) as Date,  
  3*FLOOR(CAST(EXTRACT(HOUR FROM DELIVERY_CONTACT_DATE) as int64)/3) ||"-"||  3*CEILING((1+CAST(EXTRACT(HOUR FROM DELIVERY_CONTACT_DATE) as int64))/3) as Hour,
  DELIVERY_CONTACT_DATE as DeliveryTimestamp,
  c.PROGRAM_INTERNAL_NAME_AC as ProgramName,
  d.CAMPAIGN_ID_AC as CampaignID,
  c.CAMPAIGN_NATURE as CampaignNature,
  c.CAMPAIGN_INTERNAL_NAME_AC as CampaignInternalName,
  c.CAMPAIGN_LABEL as CampaignLabel,
  DELIVERY_ID_AC as DeliveryID,
  DELIVERY_CODE as DeliveryCode,
  DELIVERY_LABEL as DeliveryLabel,
  DELIVERY_MODEL as DeliveryModel,
  DELIVERY_INTERNAL_NAME_AC as DeliveryName,
  DELIVERY_SUBJECT as Subject,
  NATURE as DeliveryNature,
  d.TYPE__PC as DeliveryType,
  case when n.triggerType is not null then n.triggerType
  when d.TYPE__PC ="Trigger" then "toClassify" end as Trigger_type,
  case when n.triggerName is not null then n.triggerName 
  when d.TYPE__PC ="Trigger" then DELIVERY_LABEL end as Trigger_name,
  Case 
    when n.locale is not null then n.locale
    when regexp_contains(DELIVERY_LABEL,"( |_)DE|DE( |_)") or regexp_contains(DELIVERY_LABEL,"(?i)(z.rich|bern|luzern|jelmoli)") then "DE"
    when regexp_contains(DELIVERY_LABEL,"( |_)FR|FR( |_)") or regexp_contains(DELIVERY_LABEL,"(?i)(gen.ve|lausanne)") then "FR"
    when regexp_contains(DELIVERY_LABEL,"( |_)IT|IT( |_)") then "IT" end as Region,      
  Case 
    when regexp_contains(lower(DELIVERY_LABEL),"manor") then "Manor"
    when regexp_contains(lower(DELIVERY_LABEL),"globus") then "Globus"
    when regexp_contains(lower(DELIVERY_LABEL),"globus") then "Jelmoli"
    when regexp_contains(lower(DELIVERY_LABEL),"impo") then "Impo"
    when regexp_contains(lower(DELIVERY_LABEL),"boutique") then "Kiehl's Boutique" else "None specified" end as Client,
  case when p.DeliveryIDPreview is not null then p.DeliveryIDPreview else DELIVERY_ID_AC end as DeliveryIDPreview,
  case when p.lastSubject is not null then p.lastSubject else DELIVERY_SUBJECT end as lastSubject
FROM
  `emea-c1-dwh-prd.switzerland_all.delivery` d
  LEFT JOIN  `emea-c1-dwh-prd.switzerland_all.campaign` c on c.CAMPAIGN_ID_AC =  d.CAMPAIGN_ID_AC
  LEFT JOIN indexPreview p on p.DeliveryID = d.DELIVERY_ID_AC
  LEFT JOIN  `emea-c1-dwh-prd.switzerland_all_local.triggerNamingCH` n on n.DeliveryLabel = d.DELIVERY_LABEL

WHERE
  DELIVERY_CHANNEL ="Email"
  and DELIVERY_CONTACT_DATE	is not null
  and d.CAMPAIGN_ID_AC <> "0"
  and cast(DELIVERY_SUCCESS as int64) >0
  and not regexp_contains(d.BRAND,"Helena Rubinstein EU|Giorgio Armani EU|Valentino EU|ValentinoEU|Derma Center")
  and date (DELIVERY_CONTACT_DATE) >= "2020-01-01"
order by DELIVERY_CONTACT_DATE desc),

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
  FROM `switzerland_all.interaction` int,
  UNNEST( int.interaction.attributes) attributes
  WHERE REGEXP_CONTAINS(int.interaction.actionType, "Own_Product|Own_Sample|Test_Product")
  GROUP BY 1,2,3,4,5,6,7,8),

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
  
  contactBetter as (SELECT  
  contextMaster.ocdMasterId,
  CASE WHEN opR.RecruitmentOriginCampaign is not null then opR.RecruitmentOriginCampaign ELSE m.acquisitionSourceCampaignName END as acquisitionSourceCampaignName,
  CASE 
    WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"unknown|apotamox|neolane|demandware|wsf") THEN "Website" 
    WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"mars|localsourcesystem|pos|btr") THEN "Offline - Mars" 
    WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"qualifio") THEN "Media - Qualifio" 
    WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"facebook") THEN "Media - Facebook" END
  as RecruitmentSource, 
  CASE WHEN DATE(m.contactAcquisitionDate )< "2019-01-01" THEN "before2019" ELSE FORMAT_DATETIME("%Y", DATETIME (m.contactAcquisitionDate )) END as YearRecruitment,
  FROM `emea-c1-dwh-prd.switzerland_all.contact_master` m
  LEFT JOIN opRecruitmentCampaign opR ON m.contextMaster.ocdMasterId = opR.ocdMasterID
  GROUP BY 1,2,3,4
  ORDER BY 1,2,3),

  tracking_log_tmp AS (
  SELECT distinct 
    OCD_CONTACT_MASTER_ID as ocdMasterId,
    DELIVERY_ID_AC as DeliveryID,
    COUNT(distinct CASE WHEN TRACKINGLOG_URL_TYPE="Open" THEN OCD_CONTACT_MASTER_ID END) AS OpenedEmail,
    COUNT(distinct CASE WHEN TRACKINGLOG_URL_TYPE="Email click" THEN OCD_CONTACT_MASTER_ID END) AS ClickedEmail,
    COUNT(distinct CASE WHEN TRACKINGLOG_URL_TYPE="Opt-out" THEN OCD_CONTACT_MASTER_ID END) AS OptoutEmail,
  FROM `emea-c1-dwh-prd.switzerland_all.tracking_log`
  GROUP BY 1,2 ORDER BY 1),

  broad_log_tmp AS (
  SELECT distinct
    OCD_CONTACT_MASTER_ID as ocdMasterId,
    DELIVERY_ID_AC as DeliveryID,
    COUNT(distinct CASE WHEN BROADLOGRCP_STATUS ="Sent" THEN OCD_CONTACT_MASTER_ID END) AS Targeted
  FROM `emea-c1-dwh-prd.switzerland_all.broad_log_rcp`
  GROUP BY 1,2 ORDER BY 1)

  SELECT distinct   
    deliveryBetter.division, 
    deliveryBetter.Brand,
    deliveryBetter.Year,
    deliveryBetter.Time,
    deliveryBetter.DayOfWeek,  
    deliveryBetter.Date,  
    deliveryBetter.Hour,
    deliveryBetter.DeliveryLabel,
    deliveryBetter.DeliveryModel,
    deliveryBetter.DeliveryName,
    deliveryBetter.Subject,
    deliveryBetter.DeliveryNature,
    deliveryBetter.DeliveryType,
    deliveryBetter.Trigger_type,
    deliveryBetter.Trigger_name,
    deliveryBetter.Region,      
    deliveryBetter.Client,
    deliveryBetter.DeliveryIDPreview,
    deliveryBetter.lastSubject,
    contactBetter.acquisitionSourceCampaignName,
    contactBetter.RecruitmentSource, 
    contactBetter.YearRecruitment,
    SUM(broad.Targeted) AS Targeted,
    SUM(tracking_log_tmp.OpenedEmail) as OpenedEmail,
    SUM(tracking_log_tmp.ClickedEmail)as ClickedEmail,
    SUM(tracking_log_tmp.OptoutEmail)as OptoutEmail,
  
  FROM broad_log_tmp broad
  LEFT JOIN contactBetter on broad.ocdMasterId = contactBetter.ocdMasterId
  LEFT JOIN deliveryBetter on broad.DeliveryID = deliveryBetter.DeliveryID
  LEFT JOIN tracking_log_tmp on (broad.DeliveryID = tracking_log_tmp.DeliveryID AND broad.ocdMasterId = tracking_log_tmp.ocdMasterId)
  where deliveryBetter.Year is not null
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  Order BY deliveryBetter.Brand, deliveryBetter.Date desc, deliveryBetter.DeliveryIDPreview