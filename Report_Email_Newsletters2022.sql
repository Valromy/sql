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


-- CampagneLabel Title	Subject	Label	Date		Delivery ID

SELECT
  c.CAMPAIGN_LABEL as CampaignLabel,
  DELIVERY_CODE as DeliveryCode,
  DELIVERY_LABEL as DeliveryLabel,
  DELIVERY_SUBJECT as Subject,  
  d.BRAND as Brand,  
  EXTRACT(Date FROM DELIVERY_CONTACT_DATE) as DeliveryDate,
  FORMAT_TIMESTAMP("%Y%m",DELIVERY_CONTACT_DATE) as YYYY_MM,
  DELIVERY_ID_AC as DeliveryID,
  SUM(cast(DELIVERY_SUCCESS as int64)) as Sent,
  SUM(cast(DELIVERY_RECIPIENTS_WHO_OPENED as int64)) as Openers,
  SUM(cast(DELIVERY_RECIPIENTS_WHO_CLICKED as int64)) as Clickers,
  SUM(cast(DELIVERY_OPTOUT as int64)) as Unsubscribes


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
  and not regexp_contains(d.BRAND,"Helena Rubinstein EU|Giorgio Armani EU|Valentino EU|ValentinoEU|Derma Center|YSL(| )EU")
  and EXTRACT(MONTH FROM DELIVERY_CONTACT_DATE) = 12
  and EXTRACT(YEAR FROM DELIVERY_CONTACT_DATE) = 2022
  and regexp_contains(lower(d.TYPE__PC),"brand")
  and cast(DELIVERY_SUCCESS as int64)>10

  group by 1,2,3,4,5,6,7,8
  order by 5,6 desc


