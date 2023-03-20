DECLARE startDate DATE DEFAULT "2021-01-01" ;
DECLARE endDate DATE DEFAULT "2023-01-01" ;


with tracking_tmp as (SELECT
    tracking.BRAND,
    FORMAT_DATETIME("%Y_%m", DATETIME (d.DELIVERY_CONTACT_DATE)) as TriggerSendDate_YYYY_mm,
    tracking.DELIVERY_ID_AC as DeliveryID,
    naming.triggerType,
    naming.triggerName,  
    naming.locale,
    tracking.TRACKINGLOG_URL_TYPE,
    tracking,TRACKINGLOG_URL_LABEL as clickLabel,	
    CASE WHEN TRACKINGLOG_URL_TYPE="Open" then OCD_CONTACT_MASTER_ID end as Openers,
    CASE WHEN TRACKINGLOG_URL_TYPE="Email click" then OCD_CONTACT_MASTER_ID end as Clickers,
  FROM `emea-c1-dwh-prd.switzerland_all.tracking_log` tracking 
  LEFT JOIN `emea-c1-dwh-prd.switzerland_all.delivery` d on d.DELIVERY_ID_AC = tracking.DELIVERY_ID_AC 
  LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.triggerNamingCH` naming on d.DELIVERY_LABEL = naming.deliveryLabel
  WHERE regexp_contains(TRACKINGLOG_URL_TYPE, "Open|Email click")
  AND DATE(d.DELIVERY_CONTACT_DATE)>= startDate AND DATE(d.DELIVERY_CONTACT_DATE)<= endDate)
  

SELECT distinct
  brand,
  TriggerSendDate_YYYY_mm,
  triggerType,
  triggerName,  
  locale,
  clicklabel,
  count(distinct Clickers) as Clickers
from tracking_tmp
where length(triggerName)>1 and clicklabel <> "Open"
group by 1,2,3,4,5,6
order by 1 asc,2 desc,3,4,7 desc


  
  