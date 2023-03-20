SELECT
  case when regexp_contains(BRAND,"YSL|Biotherm|Helena Rubinstein EU|Mugler|Giorgio Armani|Giorgio Armani EU|Lancôme|Urban Decay|Valentino EU|ValentinoEU|Kiehl's") then "Luxe"
  when regexp_contains(BRAND,"CeraVe|Derma Center|La Roche Posay|Skinceuticals|Vichy") then "ACD"
  when BRAND ="Kerastase" then "PPD" end as division, 
  BRAND as Brand,
  DELIVERY_MODEL,
  DELIVERY_INTERNAL_NAME_AC as DeliveryName,
  CAMPAIGN_ID_AC,
	DELIVERY_ID_AC as DeliveryID,
  EXTRACT(YEAR FROM DELIVERY_CONTACT_DATE) as Year,
  EXTRACT(TIME FROM DELIVERY_CONTACT_DATE) as Time,
  EXTRACT(DATE FROM DELIVERY_CONTACT_DATE) as Date,
  DELIVERY_CONTACT_DATE,
  DELIVERY_LABEL as Label,
  Case when regexp_contains(DELIVERY_LABEL,"( |_)DE|DE( |_)") or regexp_contains(DELIVERY_LABEL,"(?i)(z.rich|bern|luzern|jelmoli)") then "DE"
    when regexp_contains(DELIVERY_LABEL,"( |_)FR|FR( |_)") or regexp_contains(DELIVERY_LABEL,"(?i)(gen.ve|lausanne)") then "FR"
    when regexp_contains(DELIVERY_LABEL,"( |_)IT|IT( |_)") then "IT" end as Region,  
  Case when regexp_contains(lower(DELIVERY_LABEL),"manor") then "Manor"
  when regexp_contains(lower(DELIVERY_LABEL),"globus") then "Globus"
  when regexp_contains(lower(DELIVERY_LABEL),"boutique") then "Kiehl's Boutique" else "None specified" end as Client,
  DELIVERY_SUBJECT as Subject,
  NATURE as Nature,
  DELIVERY_PROCESSED as Processed,
  DELIVERY_SUCCESS as SuccessfulDeliveries,
  TYPE__PC,
  DELIVERY_CODE as DeliveryCode
FROM
  `emea-c1-dwh-prd.switzerland_all.delivery`
WHERE
  DELIVERY_CHANNEL ="Mobile (SMS)"
  and DELIVERY_CONTACT_DATE	is not null
  --and CAMPAIGN_ID_AC <> "0"
  and cast(DELIVERY_SUCCESS as int64) >0
order by DELIVERY_CONTACT_DATE 