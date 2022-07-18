DROP TABLE IF EXISTS smartsheet.linkedin_demographicperformancereport_jobtitle;
CREATE TABLE smartsheet.linkedin_demographicperformancereport_jobtitle
WITH 
(
  --format='textfile',
  format='parquet',
  --field_delimiter='	',
  external_location='s3://arkestral/clients/smartsheet/linkedin/demographic_performance_report/member_job_title/'
) AS 
select 
split_part(a."$path", ':', 4) as "campaignid",
--a.json_payload,
json_extract_scalar(b.json_payload, '$.name') as campaign,
json_extract_scalar(c.json_payload, '$.name') as campaign_group,
json_extract_scalar(a.json_payload, '$["pivotValue~"]["name"]["localized"]["en_US"]') as member_jobtitle,
split_part(a."$path", ':', 6) as "start",
split_part(a."$path", ':', 7) as "end",
sum(cast(json_extract_scalar(a.json_payload, '$.impressions') as bigint)) AS "impressions",
sum(cast(json_extract_scalar(a.json_payload, '$.clicks') as bigint)) AS "clicks",
sum(cast(json_extract_scalar(a.json_payload, '$.costInUsd') as double)) AS "cost_in_usd",
sum(cast(json_extract_scalar(a.json_payload, '$.externalWebsiteConversions') as bigint)) AS "landing_page_conversions",
sum(cast(json_extract_scalar(a.json_payload, '$.externalWebsitePostClickConversions') as bigint)) AS "landing_page_conversions_click",
sum(cast(json_extract_scalar(a.json_payload, '$.externalWebsitePostViewConversions') as bigint)) AS "landing_page_conversions_view",
sum(cast(json_extract_scalar(a.json_payload, '$.oneClickLeads') as bigint)) AS "leads"
from
arkestral.linkedin_demographicperformancereport_memberjobtitle a
inner join
arkestral.linkedin_matchtables_campaigns b
on
split_part(a."$path", ':', 4) = b.campaignid
INNER JOIN
arkestral.linkedin_matchtables_campaigngroups c
ON split(json_extract_scalar(b.json_payload, '$.campaignGroup'),':')[4] = c.campaigngroupid
where
a.accountid in ('504284253')
--c.campaigngroupid = '611192086'
and
json_extract_scalar(a.json_payload, '$["pivotValue~"]["name"]["localized"]["en_US"]') is not null
--json_extract_scalar(a.json_payload, '$["pivotValue~"]["localizedName"]') is null
group by
split_part(a."$path", ':', 4),
--a.json_payload,
json_extract_scalar(b.json_payload, '$.name'),
json_extract_scalar(c.json_payload, '$.name'),
json_extract_scalar(a.json_payload, '$["pivotValue~"]["name"]["localized"]["en_US"]'),
--split_part("$path", ':', 4),
split_part(a."$path", ':', 6),
split_part(a."$path", ':', 7)
order by
split_part(a."$path", ':', 6)
--limit 10
;

select * from smartsheet.linkedin_demographicperformancereport_memberjobtitle limit 100;

drop table smartsheet.linkedin_demographicperformancereport_jobtitle;
