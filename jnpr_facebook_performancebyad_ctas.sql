drop table if exists jnpr.facebook_basic_performance_report_creative;
CREATE TABLE jnpr.facebook_basic_performance_report_creative
WITH 
(
  --format='textfile',
  format='parquet',
  --field_delimiter='	',
  external_location='s3://pntheon/clients/jnpr/facebook/basic_performance_report/creative/'
) AS 
select 
--e.json_payload,
a.account_id,
json_extract_scalar(f.json_payload, '$.name') as "account_name",
json_extract_scalar(a.json_payload, '$.date_start') as "date_start",
json_extract_scalar(a.json_payload, '$.date_stop') as "date_stop",
b.campaign_id,
json_extract_scalar(b.json_payload, '$.name') as "campaign_name",
json_extract_scalar(b.json_payload, '$.objective') as "campaign_objective",
c.adset_id,
json_extract_scalar(c.json_payload, '$.name') as "adset_name",
json_extract_scalar(c.json_payload, '$.optimization_goal') as "adset_objective",
d.ad_id,
json_extract_scalar(d.json_payload, '$.name') as "ad_name",
json_extract_scalar(a.json_payload, '$.objective') as "ad_objective",
json_extract_scalar(e.json_payload, '$.name') as "creative_name",
json_extract_scalar(e.json_payload, '$.body') as "body",
json_extract_scalar(e.json_payload, '$.call_to_action_type') as "call_to_action_type",
coalesce(
json_extract_scalar(e.json_payload, '$.object_story_spec.video_data.call_to_action.value.link'), 
json_extract_scalar(e.json_payload, '$.object_story_spec.link_data.child_attachments[0].link'),
json_extract_scalar(e.json_payload, '$.object_story_spec.link_data.link')
) as "link",
coalesce(
json_extract_scalar(e.json_payload, '$.object_story_spec.link_data.message'),
json_extract_scalar(e.json_payload, '$.object_story_spec.video_data.message')
) as "message",
--json_extract_scalar(e.json_payload, '$.object_story_spec.link_data.description') as "description",
json_extract_scalar(e.json_payload, '$.object_type') as "object_type",
json_extract_scalar(e.json_payload, '$.status') as "creative_status",
json_extract_scalar(e.json_payload, '$.title') as "title",
json_extract_scalar(e.json_payload, '$.thumbnail_url') as "thumbnail",
--SUM(case
--when
--json_extract_scalar(actions, '$.action_type') = 'offsite_conversion.custom.1079187215538281'
--then 
--cast(json_extract_scalar(actions, '$.value') as bigint)
--else
--0
--end)
--as form_submits,
--SUM(case
--when
--json_extract_scalar(actions, '$.action_type') = 'offsite_conversion.custom.2165823933466487'
--then 
--cast(json_extract_scalar(actions, '$.value') as bigint)
--else
--0
--end) 
--as all_page_views,
SUM(cast(json_extract_scalar(a.json_payload, '$.impressions') as bigint)) as "impressions",
SUM(cast(json_extract_scalar(a.json_payload, '$.clicks') as bigint)) as "clicks",
SUM(cast(json_extract_scalar(a.json_payload, '$.spend') as double)) as "spend",
SUM(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.custom.1079187215538281'), x['value'],'0') as bigint))),0)) as form_submit,
SUM(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.custom.2165823933466487'), x['value'],'0') as bigint))),0)) as all_page_views,
SUM(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.custom.440038486801309'), x['value'],'0') as bigint))),0)) as chat_submit,
SUM(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.custom.315156515787890'), x['value'],'0') as bigint))),0))as hub_submit,
SUM(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.custom.1019343781608993'), x['value'],'0') as bigint))),0)) as downloads,
SUM(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'leadgen_grouped'), x['value'],'0') as bigint))),0)) as leads
from 
pntheon_clients.pntheon_facebook_basicperformancereport_creative a
--cross join unnest(cast(json_extract(a.json_payload, '$.actions') as array(json))) t(actions)
inner join
pntheon_clients.pntheon_facebook_match_tables_campaigns b
on json_extract_scalar(a.json_payload, '$.campaign_id') = json_extract_scalar(b.json_payload, '$.id') 
inner join
pntheon_clients.pntheon_facebook_match_tables_adsets c
on json_extract_scalar(a.json_payload, '$.adset_id') = json_extract_scalar(c.json_payload, '$.id') 
inner join
pntheon_clients.pntheon_facebook_match_tables_ads d
on json_extract_scalar(a.json_payload, '$.ad_id') = json_extract_scalar(d.json_payload, '$.id') 
inner join
pntheon_clients.pntheon_facebook_match_tables_creatives e
on json_extract_scalar(d.json_payload, '$.creative.id') = json_extract_scalar(e.json_payload, '$.id') 
inner join
pntheon_clients.pntheon_facebook_match_tables_accounts f
on a.account_id = f.account_id
where
json_extract_scalar(f.json_payload, '$.end_advertiser_name') = 'Juniper Networks'
or
json_extract_scalar(f.json_payload, '$.end_advertiser_name') = 'Creation Agency'
or
json_extract_scalar(f.json_payload, '$.end_advertiser_name') = 'DWA Media Singapore'
--json_extract_scalar(a.json_payload, '$.date_start') > '2020-08-15'
--coalesce(
--json_extract_scalar(e.json_payload, '$.object_story_spec.link_data.message'),
--json_extract_scalar(e.json_payload, '$.object_story_spec.video_data.message')
--) is null
group by
--e.json_payload,
a.account_id,
json_extract_scalar(f.json_payload, '$.name'),
json_extract_scalar(a.json_payload, '$.date_start'),
json_extract_scalar(a.json_payload, '$.date_stop'),
b.campaign_id,
json_extract_scalar(b.json_payload, '$.name'),
json_extract_scalar(b.json_payload, '$.objective'),
c.adset_id,
json_extract_scalar(c.json_payload, '$.name'),
json_extract_scalar(c.json_payload, '$.optimization_goal'),
d.ad_id,
json_extract_scalar(d.json_payload, '$.name'),
json_extract_scalar(a.json_payload, '$.objective'),
json_extract_scalar(e.json_payload, '$.name'),
json_extract_scalar(e.json_payload, '$.body'),
json_extract_scalar(e.json_payload, '$.call_to_action_type'),
coalesce(
json_extract_scalar(e.json_payload, '$.object_story_spec.video_data.call_to_action.value.link'), 
json_extract_scalar(e.json_payload, '$.object_story_spec.link_data.child_attachments[0].link'),
json_extract_scalar(e.json_payload, '$.object_story_spec.link_data.link')
),
coalesce(
json_extract_scalar(e.json_payload, '$.object_story_spec.link_data.message'),
json_extract_scalar(e.json_payload, '$.object_story_spec.video_data.message')
),
--json_extract_scalar(e.json_payload, '$.object_story_spec.link_data.description'),
json_extract_scalar(e.json_payload, '$.object_type'),
json_extract_scalar(e.json_payload, '$.status'),
json_extract_scalar(e.json_payload, '$.title'),
json_extract_scalar(e.json_payload, '$.thumbnail_url')
;

select 
json_extract_scalar(json_payload, '$.name'), 
json_extract_scalar(json_payload, '$.end_advertiser_name') 
from 
pntheon_clients.pntheon_facebook_match_tables_accounts
group by
json_extract_scalar(json_payload, '$.name'), 
json_extract_scalar(json_payload, '$.end_advertiser_name') 
;


select
distinct(account_name)
from
jnpr.facebook_basic_performance_report_creative
;

select "$path", max(date_start), max(date_stop)
from jnpr.facebook_basic_performance_report_creative
--where
--date_stop not like '2020%'
group by 
"$path"
;

select 
--ad_id,
SUM(spend) as spend,
SUM(form_submit) as form_submit,
SUM(all_page_views) as all_page_views
--SUM(form_submits) as form_submits,
--SUM(all_page_views) as all_page_views
from
jnpr.facebook_basic_performance_report_creative
--group by
--ad_id
--order by
--form_submit 
--DESC
;

select
split_part("$path", ':', 5) as "date",
count(distinct(split_part("$path", ':', 3))) as "accountids"
from
pntheon_clients.pntheon_facebook_basicperformancereport_creative a
group by
split_part("$path", ':', 5)
--split_part("$path", ':', 3)
order by 
split_part("$path", ':', 5)
--split_part("$path", ':', 3)
asc;

select
distinct(json_payload)
from 
pntheon_clients.pntheon_facebook_basicperformancereport_creative a
where
json_extract_scalar(a.json_payload, '$.date_start') is null;

select 
distinct(link)
from 
jnpr.facebook_basic_performance_report_creative 
where 
link not like '%jtrkId=%'
--limit 100
;

select count(account_id) from jnpr.facebook_basic_performance_report_creative;

select count(user_id) from pntheon_clients.pntheon_facebook_basicperformancereport_creative;


--limit 1;

select json_extract(json_payload, '$.actions') from pntheon_clients.facebook_basic_performance_report_creative limit 100;

select 
i AS array_items
from 
pntheon_clients.pntheon_facebook_basicperformancereport_creative
CROSS JOIN UNNEST(json_extract_scalar(json_payload, '$.actions')) AS t(i)
WHERE contains(i, '1079187215538281')
limit 100
;

with dataset as (
select
cast(json_extract(json_payload, '$.actions') as ARRAY(MAP(VARCHAR, JSON))) as actions_array
--json_extract(json_payload, '$.actions.action_type'),
from
pntheon_clients.pntheon_facebook_basicperformancereport_creative
limit
100)
select
actions_array,
actions
from
dataset,
unnest (actions_array) AS t(actions)
;

select
actions 
from
pntheon_clients.pntheon_facebook_basicperformancereport_creative,
unnest (cast(json_extract(json_payload, '$.actions')) as array) as t(actions)
;

select
--cast(json_extract(json_payload, '$.actions') as array(json)) as items,
json_extract_scalar(actions, '$.action_type') as action_type,
SUM(case
when
json_extract_scalar(actions, '$.action_type') = 'offsite_conversion.custom.1079187215538281'
then 
json_extract_scalar(actions, '$.value')
end)
as form_submits,
SUM(case
when
json_extract_scalar(actions, '$.action_type') = 'offsite_conversion.custom.2165823933466487'
then 
json_extract_scalar(actions, '$.value')
end)
as all_page_views
from
pntheon_clients.pntheon_facebook_basicperformancereport_creative,
UNNEST(cast(json_extract(json_payload, '$.actions') as array(json))) t(actions)
--json_array_contains(cast(json_extract(json_payload, '$.actions') as array(json)), 'link_clicks')
--where
--json_extract_scalar(actions, '$.action_type') = 'link_click'
group by 
json_extract_scalar(actions, '$.action_type')
--limit 
--10
;

select * from pntheon_clients.pntheon_facebook_match_tables_conversions where conversion_id =  '440038486801309' order by account_id desc;

select distinct(json_extract_scalar(json_payload, '$.business.name')) from pntheon_clients.pntheon_facebook_match_tables_conversions;

select * from pntheon_clients.pntheon_facebook_match_tables_conversions where json_extract_scalar(json_payload, '$.business.name')  = 'Our Body Electric Fitness';

select
	a.account_id,
	json_extract_scalar(f.json_payload,
	'$.name') as "account_name",
	json_extract_scalar(a.json_payload,
	'$.date_start') as "date_start",
	json_extract_scalar(a.json_payload,
	'$.date_stop') as "date_stop",
	b.campaign_id,
	json_extract_scalar(b.json_payload,
	'$.name') as "campaign_name",
	json_extract_scalar(b.json_payload,
	'$.objective') as "campaign_objective",
	c.adset_id,
	json_extract_scalar(c.json_payload,
	'$.name') as "adset_name",
	json_extract_scalar(c.json_payload,
	'$.optimization_goal') as "adset_objective",
	d.ad_id,
	json_extract_scalar(d.json_payload,
	'$.name') as "ad_name",
	json_extract_scalar(a.json_payload,
	'$.objective') as "ad_objective",
	json_extract_scalar(e.json_payload,
	'$.name') as "creative_name",
	json_extract_scalar(e.json_payload,
	'$.body') as "body",
	json_extract_scalar(e.json_payload,
	'$.call_to_action_type') as "call_to_action_type",
	coalesce(json_extract_scalar(e.json_payload,
	'$.object_story_spec.video_data.call_to_action.value.link'),
	json_extract_scalar(e.json_payload,
	'$.object_story_spec.link_data.child_attachments[0].link'),
	json_extract_scalar(e.json_payload,
	'$.object_story_spec.link_data.link') ) as "link",
	coalesce(json_extract_scalar(e.json_payload,
	'$.object_story_spec.link_data.message'),
	json_extract_scalar(e.json_payload,
	'$.object_story_spec.video_data.message') ) as "message",
	json_extract_scalar(e.json_payload,
	'$.object_type') as "object_type",
	json_extract_scalar(e.json_payload,
	'$.status') as "creative_status",
	json_extract_scalar(e.json_payload,
	'$.title') as "title",
	json_extract_scalar(e.json_payload,
	'$.thumbnail_url') as "thumbnail",
	sum(cast(json_extract_scalar(a.json_payload, '$.impressions') as bigint)) as "impressions",
	sum(cast(json_extract_scalar(a.json_payload, '$.clicks') as bigint)) as "clicks",
	sum(cast(json_extract_scalar(a.json_payload, '$.spend') as double)) as "spend",
	sum(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.custom.398652818056280'), x['value'], '0') as bigint))), 0)) as "obé Start Trial CBC CC 2021",
	sum(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.custom.738735523385803'), x['value'], '0') as bigint))), 0)) as "Trial - Account Conversion",
	sum(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.custom.2598494360462048'), x['value'], '0') as bigint))), 0)) as "Scary Mommy - Free Trial",
	sum(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.custom.2348286662103144'), x['value'], '0') as bigint))), 0)) as "Onboarding Subscription - Started Free Trial",
	sum(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.custom.665631197667657'), x['value'], '0') as bigint))), 0)) as "obe Start Trial CBC CC",
	sum(coalesce(array_max(transform(cast(json_extract(a.json_payload, '$.actions') as array<map<varchar, varchar>>), x -> cast(if(regexp_like(x['action_type'], 'offsite_conversion.fb_pixel_complete_registration'), x['value'], '0') as bigint))), 0)) as "Registrations Completed"
from
	pntheon_clients.pntheon_facebook_basicperformancereport_creative a
inner join pntheon_clients.pntheon_facebook_match_tables_campaigns b on
	json_extract_scalar(a.json_payload,
	'$.campaign_id') = json_extract_scalar(b.json_payload,
	'$.id')
inner join pntheon_clients.pntheon_facebook_match_tables_adsets c on
	json_extract_scalar(a.json_payload,
	'$.adset_id') = json_extract_scalar(c.json_payload,
	'$.id')
inner join pntheon_clients.pntheon_facebook_match_tables_ads d on
	json_extract_scalar(a.json_payload,
	'$.ad_id') = json_extract_scalar(d.json_payload,
	'$.id')
inner join pntheon_clients.pntheon_facebook_match_tables_creatives e on
	json_extract_scalar(d.json_payload,
	'$.creative.id') = json_extract_scalar(e.json_payload,
	'$.id')
inner join pntheon_clients.pntheon_facebook_match_tables_accounts f on
	a.account_id = f.account_id
where
	f.account_id = 'act_2101634313474413'
group by
	a.account_id,
	json_extract_scalar(f.json_payload,
	'$.name'),
	json_extract_scalar(a.json_payload,
	'$.date_start'),
	json_extract_scalar(a.json_payload,
	'$.date_stop'),
	b.campaign_id,
	json_extract_scalar(b.json_payload,
	'$.name'),
	json_extract_scalar(b.json_payload,
	'$.objective'),
	c.adset_id,
	json_extract_scalar(c.json_payload,
	'$.name'),
	json_extract_scalar(c.json_payload,
	'$.optimization_goal'),
	d.ad_id,
	json_extract_scalar(d.json_payload,
	'$.name'),
	json_extract_scalar(a.json_payload,
	'$.objective'),
	json_extract_scalar(e.json_payload,
	'$.name'),
	json_extract_scalar(e.json_payload,
	'$.body'),
	json_extract_scalar(e.json_payload,
	'$.call_to_action_type'),
	coalesce(json_extract_scalar(e.json_payload,
	'$.object_story_spec.video_data.call_to_action.value.link'),
	json_extract_scalar(e.json_payload,
	'$.object_story_spec.link_data.child_attachments[0].link'),
	json_extract_scalar(e.json_payload,
	'$.object_story_spec.link_data.link') ),
	coalesce(json_extract_scalar(e.json_payload,
	'$.object_story_spec.link_data.message'),
	json_extract_scalar(e.json_payload,
	'$.object_story_spec.video_data.message') ),
	json_extract_scalar(e.json_payload,
	'$.object_type'),
	json_extract_scalar(e.json_payload,
	'$.status'),
	json_extract_scalar(e.json_payload,
	'$.title'),
	json_extract_scalar(e.json_payload,
	'$.thumbnail_url')


select
	*
from
	pntheon_clients.pntheon_facebook_basicperformancereport_creative
where
account_id = 'act_2101634313474413';

select distinct(account_id)
from 
pntheon_clients.pntheon_facebook_match_tables_ads;


