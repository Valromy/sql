with beautyprofile as (SELECT 
context.ocdMasterId,
profile.skinDescription.sunReaction,
routine.skincareRoutine.skincareMoment,
routine.skincareRoutine.skincareEyeUse,
priority.skinPriority.skinMotivation,
routine.skincareRoutine.sunExposureFrequency,
preference.interest.makeupInterest,
preference.interest.skincareInterest,
preference.interest.fragranceInterest,
FROM `emea-c1-dwh-prd.switzerland_all.contact_beautyprofile`),

skinattributes as (SELECT 
context.ocdMasterId,
skinattributes.skinZone,
skinattributes.skinSensitivity,
skinattributes.skintype,
FROM `emea-c1-dwh-prd.switzerland_all.contact_beautyprofile`, unnest(profile.skinDescription.skinZoneAttributesList) as skinattributes),

skinImprov as (SELECT 
context.ocdMasterId,
skinPriorityImprov.concernType as skinConcernType,
skinPriorityImprov.concernDescription as skinConcernDescription,
FROM `emea-c1-dwh-prd.switzerland_all.contact_beautyprofile`, unnest(priority.skinPriority.concernImprovementGoals) as skinPriorityImprov)

select 
case when regexp_contains(lower(m.contextMaster.brand),"ysl|biotherm|rubinstein|mugler|armani|lanc.me|urban decay|valentino|kiehl('|)s") then "Luxe"
  when regexp_contains(lower(m.contextMaster.brand),"cerave|derma center|posay|lrp|skinceuticals|vichy") then "ACD"
  when regexp_contains(lower(m.contextMaster.brand), "k(e|é)rastase") then "PPD" end as division, 
case when m.contextMaster.brand = "LaRochePosay" then "LRP"
when m.contextMaster.brand = "Kerastase" then "Kérastase"
when m.contextMaster.brand = "Lancome" then "Lancôme"
when m.contextMaster.brand = "Kiehls" then "Kiehl's"
when m.contextMaster.brand = "GiorgioArmani" then "Armani"
when m.contextMaster.brand = "Ysl" then "YSL" else m.contextMaster.brand end as brand,
case WHEN m.ageGroup is null THEN "Unknown" ELSE m.ageGroup END as ageGroup,
-- check all dimensions of beauty profile for non null values 
case when 
b.sunReaction is not null or
b.skincareMoment is not null or
b.skincareEyeUse is not null or
b.skinMotivation is not null or
b.sunExposureFrequency is not null or
b.makeupInterest is not null or
b.skincareInterest is not null or
b.fragranceInterest is not null or
a.skinZone is not null or
a.skinSensitivity is not null or
a.skintype is not null or
i.skinConcernType is not null or
i.skinConcernDescription is not null then true else false end as QualifiedBeautyProfile,
-- list beautyprofile potential values
b.sunReaction,
b.skincareMoment,
b.skincareEyeUse,
b.skinMotivation,
b.sunExposureFrequency,
b.makeupInterest,
b.skincareInterest,
b.fragranceInterest,
a.skinZone,
a.skinSensitivity,
a.skintype,
i.skinConcernType,
i.skinConcernDescription,
count(distinct m.contextMaster.ocdMasterId) as Contacts,
count(distinct CASE WHEN m.isContactableEmail THEN m.contextMaster.ocdMasterId END) AS ContactableEmail,
count(distinct CASE WHEN m.isContactablePostal THEN m.contextMaster.ocdMasterId END) AS ContactablePost,
count(distinct CASE WHEN m.isContactableSms THEN m.contextMaster.ocdMasterId END) AS ContactableSMS,
FROM `emea-c1-dwh-prd.switzerland_all.contact_master` m
LEFT JOIN beautyprofile b on b.ocdMasterID = m.contextMaster.ocdMasterId
LEFT JOIN skinattributes a on a.ocdMasterID = m.contextMaster.ocdMasterId
LEFT JOIN skinImprov i on i.ocdMasterID = m.contextMaster.ocdMasterId

WHERE not m.genericContact
and not m.anonymizedContact
and not m.cCareNonOptinContact
and m.division <> "Consumer Products Division"
and m.acquisitionSourcePlatform<> "customercare"
and not regexp_contains(lower(m.contextMaster.brand), "valentinoeu|valentino eu|helenarubinsteineu|giorgioarmanieu|urbandecay|ysleu")
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
having QualifiedBeautyProfile
order by 1,2

