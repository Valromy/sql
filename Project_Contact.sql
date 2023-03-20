DECLARE brands STRING DEFAULT "Kiehls";

SELECT distinct
  SHA256(CAST(contextMaster.ocdMasterId AS STRING)) AS CustomerId,
  DATE(contactAcquisitionDate) AS contactAcquisitionDate,
  CASE 
    WHEN REGEXP_CONTAINS(preferences.language.language, "fr") THEN "fr" 
    WHEN REGEXP_CONTAINS(preferences.language.language, "it") THEN "it" 
    ELSE "de" 
  END AS Language, 
  identityInfo.gender.gender,
  CASE 
    WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform), "unknown|apotamox|neolane|demandware|wsf") THEN "Website" 
    WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform), "mars|localsourcesystem|pos|btr") THEN "Offline" 
    WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform), "facebook|qualifio") THEN "Media" 
  END AS RecruitmentSource, 
  contactInfo.postal.zipCode,
  contactInfo.postal.city,
  birthday,
  CASE 
    WHEN SUBSTR(emailAddress, INSTR(emailAddress, '@') + 1) IN ("gmail.com", "hotmail.com", "bluewin.ch", "gmx.ch", "yahoo.com", "icloud.com", "outlook.com", "hotmail.ch", "yahoo.fr", "hotmail.fr", "sunrise.ch", "gmx.net", "yahoo.de", "hispeed.ch", "bluemail.ch") THEN SUBSTR(emailAddress, INSTR(emailAddress, '@') + 1)
    ELSE "other"
  END AS emailDomain,
  CASE 
    WHEN SUBSTR(emailAddress, INSTR(emailAddress, '@') + 1) LIKE '%.de' OR SUBSTR(emailAddress, INSTR(emailAddress, '@') + 1) LIKE '%.fr' OR SUBSTR(emailAddress, INSTR(emailAddress, '@') + 1) LIKE '%.it' THEN "frdeit_domain"
    WHEN SUBSTR(emailAddress, INSTR(emailAddress, '@') + 1) LIKE '%.ch' THEN "swiss_domain"
    ELSE "international_domain"
  END AS emailDomainType
FROM `emea-c1-dwh-prd.switzerland_all.contact_master`
WHERE 
  REGEXP_CONTAINS(contextMaster.brand, brands)
--  AND NOT anonymizedContact
  AND NOT genericContact
  AND NOT cCareNonOptinContact
  AND acquisitionSourcePlatform <> "customercare"

ORDER BY 1
