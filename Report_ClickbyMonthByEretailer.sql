DECLARE startDate DATE DEFAULT "2019-01-01" ;
DECLARE endDate DATE DEFAULT "2022-12-31" ;
-- UPDATE ICI GITAN
DECLARE strYTD STRING DEFAULT ".*_.*";

with tracking_tmp as (SELECT
    tracking.BRAND,
    FORMAT_DATETIME("%Y", DATETIME (d.DELIVERY_CONTACT_DATE)) as SendDate_YYYY,    
    FORMAT_DATETIME("%Y_%m", DATETIME (d.DELIVERY_CONTACT_DATE)) as SendDate_YYYY_mm,    
    CASE 
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"impo") THEN "impo.ch" 
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"globus") THEN "globus.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"jelmoli") THEN "jelmoli.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"manor") THEN "manor.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"haar.shop") THEN "haarshop.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"perfecthair") THEN "perfecthair.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"marionnaud") THEN "marionnaud.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"dermanence") THEN "dermanence.com"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"commerce.connector") THEN "commerce-connector.com"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"amavita") THEN "amavita.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"adlershop") THEN "adlershop.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"clickandcare") THEN "clickandcare.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE	,"coopvitality") THEN "coopvitality.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "zurrose") THEN "zurrose-shop.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "sunstore.ch") THEN "sunstore.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "laroche-posay.ch|spotscan") THEN "laroche-posay.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "vichy.ch") THEN "vichy.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "skinceuticals.ch") THEN "skinceuticals.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "kiehls.ch") THEN "kiehls.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "mugler.ch") THEN "mugler.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "qualifio|beauty.campaigns.com") THEN "qualifio"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "armanibeauty.ch") THEN "armanibeauty.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "avalentino-beauty.ch") THEN "valentino-beauty.ch"      
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "kerastase.ch") THEN "kerastase.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "facebook.com") THEN "facebook.com"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "instagram.com") THEN "instagram.com"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "(youtube.com|youtu.be)") THEN "youtube.com"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "urbandecay.ch") THEN "urbandecay.ch"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "twitter.com") THEN "twitter.com"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "loreal.(ch|com)") THEN "loreal corp"
      WHEN REGEXP_CONTAINS(TRACKINGLOG_URL_SOURCE, "pinterest") THEN "pinterest.com"
      else "other"
    END as redirName,
    CASE WHEN TRACKINGLOG_URL_TYPE="Open" then OCD_CONTACT_MASTER_ID end as Openers,
    CASE WHEN TRACKINGLOG_URL_TYPE="Email click" then OCD_CONTACT_MASTER_ID end as Clickers,
  FROM `emea-c1-dwh-prd.switzerland_all.tracking_log` tracking 
  LEFT JOIN `emea-c1-dwh-prd.switzerland_all.delivery` d on d.DELIVERY_ID_AC = tracking.DELIVERY_ID_AC 
  --LEFT JOIN `emea-c1-dwh-prd.switzerland_all_local.triggerNamingCH` naming on d.DELIVERY_LABEL = naming.deliveryLabel
  WHERE regexp_contains(TRACKINGLOG_URL_TYPE, "Open|Email click")
  AND DATE(d.DELIVERY_CONTACT_DATE)>= startDate AND DATE(d.DELIVERY_CONTACT_DATE)<= endDate)
  

SELECT distinct
  Case WHEN regexp_contains(brand,"Biotherm|Giorgio Armani|Kiehl's|Lancôme|Mugler|Urban Decay|YSL") then "Luxe"
  WHEN regexp_contains(brand,"CeraVe|La Roche Posay|Skinceuticals|Vichy") then "ACD"
  ELSE "PPD" END as division,
  brand,  
  SendDate_YYYY,
  SendDate_YYYY_mm,
  case when REGEXP_CONTAINS(SendDate_YYYY_mm, strYTD) then "YTD" END as YTD,
  case WHEN REGEXP_CONTAINS(redirName,"adlershop.ch|amavita.ch|clickandcare.ch|coop.ch|coopvitality.ch|galaxus.ch|globus.ch|haarshop.ch|impo.ch|manor.ch|marionnaud.ch|migros.ch|perfecthair.ch|sunstore.ch|zurrose-shop.ch|impo.ch|perfecthair.ch|jelmoli.ch") THEN "eRetail" ELSE "owned" end as redirType,
  redirName,
  count(distinct Clickers) as Clickers,
  Count(distinct CASE WHEN SendDate_YYYY = "2019" THEN  Clickers
      ELSE NULL END) AS Clicks_2019,
  Count(distinct CASE WHEN SendDate_YYYY = "2020" AND REGEXP_CONTAINS(SendDate_YYYY_mm, strYTD) THEN  Clickers
      ELSE NULL END) AS Clicks_YTD_2020,
  Count(distinct CASE WHEN SendDate_YYYY = "2020" THEN  Clickers
      ELSE NULL END) AS Clicks_2020,
  Count(distinct CASE WHEN SendDate_YYYY = "2021" AND REGEXP_CONTAINS(SendDate_YYYY_mm, strYTD) THEN  Clickers
      ELSE NULL END) AS Clicks_YTD_2021,
    Count(distinct CASE WHEN SendDate_YYYY = "2021" THEN  Clickers
      ELSE NULL END) AS Clicks_2021,
  Count(distinct CASE WHEN SendDate_YYYY = "2022" AND REGEXP_CONTAINS(SendDate_YYYY_mm, strYTD) THEN  Clickers
      ELSE NULL END) AS Clicks_YTD_2022
from tracking_tmp
where not regexp_contains(brand,"HelenaRubinsteinEU|ValentinoEU")

group by 1,2,3,4,5,6,7
order by 1 asc