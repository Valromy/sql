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


--Brand/type/year/month/sent
SELECT
  case when regexp_contains(d.BRAND,"YSL|Biotherm|Helena Rubinstein EU|Mugler|Giorgio Armani|Giorgio Armani EU|Lancôme|Urban Decay|Valentino EU|ValentinoEU|Kiehl's") then "Luxe"
  when regexp_contains(d.BRAND,"CeraVe|Derma Center|La Roche Posay|Skinceuticals|Vichy") then "ACD"
  when d.BRAND ="Kerastase" then "PPD" end as division, 
  case 
    when d.BRAND="La Roche Posay" then "LRP"
    when d.BRAND="Kiehl's" then "Kiehls"
    when d.BRAND="Giorgio Armani" then "Armani"
    when d.BRAND="Kérastase" then "Kerastase"
  else d.BRAND end as Brand,
  d.TYPE__PC as DeliveryType,
  EXTRACT(YEAR FROM DELIVERY_CONTACT_DATE) as Year,
  EXTRACT(MONTH FROM DELIVERY_CONTACT_DATE) as Month,
  SUM(cast(DELIVERY_SUCCESS as int64)) as Delivered,
FROM
  `emea-c1-dwh-prd.switzerland_all.delivery` d

WHERE
  DELIVERY_CHANNEL ="Email"
  and DELIVERY_CONTACT_DATE	is not null
  --and d.CAMPAIGN_ID_AC <> "0"
  and cast(DELIVERY_SUCCESS as int64) >0
  and not regexp_contains(d.BRAND,"Helena Rubinstein EU|Giorgio Armani EU|Valentino EU|ValentinoEU|Derma Center")
  and date (DELIVERY_CONTACT_DATE) >= "2020-01-01"
  and (d.TYPE__PC = "Trigger" or cast(DELIVERY_SUCCESS as int64)>10)

Group by 1,2,3,4,5
order by  1,2,3,4,5