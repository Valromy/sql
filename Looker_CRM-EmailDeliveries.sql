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
  --and d.CAMPAIGN_ID_AC <> "0"
  and cast(DELIVERY_SUCCESS as int64) >0
  and TYPE__PC = "Trigger"),

helpPreview as (select keyTrigger, DeliveryIDPreview, lastSubject
from (select DeliveryID as DeliveryIDPreview,Subject as lastSubject, keyTrigger,DeliveryDate, ROW_NUMBER() OVER (PARTITION BY keyTrigger ORDER BY DeliveryDate DESC ) AS DeliveryRank ,
FROM localNaming where keyTrigger is not null) where DeliveryRank =1),

indexPreview as (select n.DeliveryID, h.DeliveryIDPreview, h.lastSubject
from localNaming n
LEFT JOIN helpPreview h on h.keyTrigger = n.keyTrigger),

firstCampaignDate as (select CampaignID, Date from
(select distinct d.CAMPAIGN_ID_AC as CampaignID, EXTRACT(DATE FROM DELIVERY_CONTACT_DATE) as Date, ROW_NUMBER() OVER (PARTITION BY d.CAMPAIGN_ID_AC ORDER BY EXTRACT(DATE FROM DELIVERY_CONTACT_DATE) ASC) as rn,
from `emea-c1-dwh-prd.switzerland_all.delivery` d  LEFT JOIN  `emea-c1-dwh-prd.switzerland_all.campaign` c on c.CAMPAIGN_ID_AC =  d.CAMPAIGN_ID_AC) where rn=1)

SELECT
  case when regexp_contains(d.BRAND,"YSL|Biotherm|Helena Rubinstein EU|Mugler|Giorgio Armani|Giorgio Armani EU|Lancôme|Urban Decay|Valentino EU|ValentinoEU|Kiehl's") then "Luxe"
  when regexp_contains(d.BRAND,"CeraVe|Derma Center|La Roche Posay|Skinceuticals|Vichy") then "ACD"
  when regexp_contains(lower(d.BRAND),"k.rastase") then "PPD" end as division, 
  d.BRAND as Brand,
  EXTRACT(YEAR FROM DELIVERY_CONTACT_DATE) as Year,
  FORMAT_TIMESTAMP("%y_%W",DELIVERY_CONTACT_DATE) as Year_Week,
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
  f.Date as CampaignDate,
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
	when regexp_contains(lower(DELIVERY_LABEL),"haar(|.)shop") then "Haarshop"
	when regexp_contains(lower(DELIVERY_LABEL),"jelmoli") then "Jelmoli"
	when regexp_contains(lower(DELIVERY_LABEL),"mrd|mario(nn|n)au") then "Marionnaud"
    when regexp_contains(lower(DELIVERY_LABEL),"boutique") then "Kiehl's Boutique"
	when d.BRAND = "Kiehl's" then "Kiehl's eD2C"
	when d.BRAND = "Giorgio Armani" and regexp_contains(lower(DELIVERY_LABEL),"review") then "Impo"
	when d.BRAND = "Biotherm" then "Impo"
	else "None specified" end as Client,
  case when p.DeliveryIDPreview is not null then p.DeliveryIDPreview else DELIVERY_ID_AC end as DeliveryIDPreview,
  case when p.lastSubject is not null then p.lastSubject else DELIVERY_SUBJECT end as lastSuject,
  cast(DELIVERY_INITIAL_TARGET as int64)as Targeted,
  cast(DELIVERY_PROCESSED as int64) as Processed,
  cast(DELIVERY_SUCCESS as int64) as Delivered,
  cast(DELIVERY_RECIPIENTS_WHO_OPENED as int64) as Openers,
  cast(DELIVERY_NB_OPEN as int64) as Opens,
  cast(DELIVERY_RECIPIENTS_WHO_CLICKED as int64) as Clickers,
  cast(DELIVERY_NB_CLICK as int64) as Clicks,
  cast(DELIVERY_OPTOUT as int64) as Unsubscriptions,
FROM
  `emea-c1-dwh-prd.switzerland_all.delivery` d
  LEFT JOIN  `emea-c1-dwh-prd.switzerland_all.campaign` c on c.CAMPAIGN_ID_AC =  d.CAMPAIGN_ID_AC
  LEFT JOIN indexPreview p on p.DeliveryID = d.DELIVERY_ID_AC
  LEFT JOIN firstCampaignDate f on d.CAMPAIGN_ID_AC =  f.CampaignID
  LEFT JOIN  `emea-c1-dwh-prd.switzerland_all_local.triggerNamingCH` n on n.DeliveryLabel = d.DELIVERY_LABEL

WHERE
  DELIVERY_CHANNEL ="Email"
  and DELIVERY_CONTACT_DATE	is not null
  --and d.CAMPAIGN_ID_AC <> "0"
  and cast(DELIVERY_SUCCESS as int64) >0
  and not regexp_contains(d.BRAND,"Helena Rubinstein EU|Giorgio Armani EU|Valentino EU|ValentinoEU|Derma Center")
  and date (DELIVERY_CONTACT_DATE) >= "2020-01-01"
  and (d.TYPE__PC = "Trigger" or cast(DELIVERY_SUCCESS as int64)>10)