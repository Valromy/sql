WITH contactv as (SELECT distinct
case when c.context.brand = "LaRochePosay" then "LRP"
when c.context.brand = "Kerastase" then "Kérastase"
when c.context.brand = "Lancome" then "Lancôme"
when c.context.brand = "Kiehls" then "Kiehl's"
when c.context.brand = "GiorgioArmani" then "Armani"
when c.context.brand = "Ysl" then "YSL"
else c.context.brand end as brand,
c.context.ocdMasterId, 
c.context.versionId, 
CASE 
  WHEN REGEXP_CONTAINS(lower(c.source.sourceName),"unknown|apotamox|neolane|demandware|wsf") THEN "Website" 
  WHEN REGEXP_CONTAINS(lower(c.source.sourceName),"mars|localsourcesystem|pos|btr") THEN "Mars" 
  WHEN REGEXP_CONTAINS(lower(c.source.sourceName),"qualifio") THEN "Qualifio" 
  WHEN REGEXP_CONTAINS(lower(c.source.sourceName),"facebook") THEN "Facebook"
  WHEN REGEXP_CONTAINS(lower(c.source.sourceName),"sampler") THEN "Sampler" else INITCAP(c.source.sourceName) END as sourceName,
CASE 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"unknown|apotamox|neolane|demandware|wsf") THEN "Website" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"mars|localsourcesystem|pos|btr") THEN "Mars" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"qualifio") THEN "Qualifio" 
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"facebook") THEN "Facebook"
  WHEN REGEXP_CONTAINS(lower(acquisitionSourcePlatform),"sampler") THEN "Sampler" else INITCAP(acquisitionSourcePlatform) END as acquisitionSourcePlatform,
acquisitionSourceCampaignName as RecruitmentCampaign, 
c.source.sourceCampaignName as Campaign,
EXTRACT(DATE FROM c.context.creationDate) as UpdateDate,
EXTRACT(DATE FROM m.contactAcquisitionDate) as AcquisitionDate,
CASE WHEN c.identityInfoDqm.birthdateDqm.birthDay IS NOT NULL THEN TRUE ELSE FALSE END AS KnownBirthdate,
CASE WHEN REGEXP_CONTAINS(c.identityInfoDqm.genderDqm.genderDqm,"F|M") THEN TRUE ELSE FALSE END AS KnownGender

FROM `emea-c1-dwh-prd.switzerland_all.contact_versioning` c 
left join `emea-c1-dwh-prd.switzerland_all.contact_master` m on m.contextMaster.ocdMasterId = c.context.ocdMasterId

WHERE not m.genericContact
AND not m.cCareNonOptinContact
AND not m.anonymizedContact
AND m.division <> "Consumer Products Division"
AND lower(c.source.sourceName) <> "customercare"
AND lower(m.acquisitionSourcePlatform)<> "customercare"
and not regexp_contains(lower(m.contextMaster.brand), "valentinoeu|valentino eu|helenarubinsteineu|giorgioarmanieu|urbandecay|ysleu")),

QualifiedBirth as (select distinct brand, ocdMasterId, UpdateDate, INITCAP(sourceName) as sourceName, Campaign FROM(select brand,ocdMasterId,UpdateDate,sourceName,Campaign, ROW_NUMBER() OVER (PARTITION BY ocdMasterId ORDER BY UpdateDate ASC ) AS rn from contactv where KnownBirthdate) where rn =1),
QualifiedGender as (select distinct brand,ocdMasterId,UpdateDate, INITCAP(sourceName) as sourceName, Campaign FROM(select brand,ocdMasterId,UpdateDate,sourceName,Campaign, ROW_NUMBER() OVER (PARTITION BY ocdMasterId ORDER BY UpdateDate ASC ) AS rn from contactv where KnownGender) where rn =1),

CountRecruitment as (select brand, acquisitionSourcePlatform as sourceName, RecruitmentCampaign as Campaign, AcquisitionDate as UpdateDate, count(distinct ocdMasterId) as Contacts from contactv group by 1,2,3,4),
CountBirth as (select brand, sourceName, Campaign, UpdateDate, count(distinct ocdMasterId) as ContactsQualifBirthdate from QualifiedBirth group by 1,2,3,4),
CountGender as (select brand, sourceName, Campaign, UpdateDate, count(distinct ocdMasterId) as ContactsQualifGender from QualifiedGender group by 1,2,3,4),
-- logic union all pour les date + sources + brand

indexData as (select * from (
SELECT brand, UpdateDate, sourceName, Campaign FROM CountRecruitment 
union distinct
SELECT brand, UpdateDate, sourceName, Campaign FROM CountGender
union distinct
SELECT brand, UpdateDate, sourceName, Campaign FROM CountBirth) order by 1,2 desc,3)

select 
  case when regexp_contains(i.brand,"YSL|Biotherm|Helena Rubinstein EU|Mugler|Giorgio Armani|Armani|Giorgio Armani EU|Lancôme|Urban Decay|Valentino EU|ValentinoEU|Kiehl('|)s") then "Luxe"
  when regexp_contains(lower(i.brand),"cerave|derma center|la roche.posay|lrp|skinceuticals|vichy") then "ACD"
  when regexp_contains(lower(i.brand), "k(e|é)rastase") then "PPD" end as division, 
i.brand,
i.UpdateDate as Date,
i.sourceName as Source,
i.Campaign as Campaign,
sum(r.Contacts) as Recruited_Contacts,
sum(g.ContactsQualifGender) as Qualified_Gender,
sum(b.ContactsQualifBirthdate) as Qualified_Birthdate,

from indexdata i
LEFT JOIN CountRecruitment r on (r.brand = i.brand and i.sourceName = r.sourceName and i.Campaign = r.Campaign and i.UpdateDate = r.UpdateDate)
LEFT JOIN CountBirth b on (b.brand = i.brand and i.sourceName = b.sourceName and i.Campaign = b.Campaign and i.UpdateDate = b.UpdateDate)
LEFT JOIN CountGender g on (g.brand = i.brand and i.sourceName = g.sourceName and i.Campaign = g.Campaign and i.UpdateDate = g.UpdateDate)
group by 1,2,3,4,5
