drop table if exists obefitness.dcm_basic_performance_report_impressions;
CREATE TABLE obefitness.dcm_basic_performance_report_impressions
WITH 
(
  --format='textfile',
  format='parquet',
  --field_delimiter='	',
  external_location='s3://pntheon/clients/apollo/obefitness/dcm/basic_performance_report/impressions/'
) AS 
select
date_format(from_unixtime(cast(event_time as bigint)/1000000, 'America/New_York'), '%Y-%m-%d') as "date",
b.advertiser,
c.campaign,
d."site_(dcm)" as site,
e.placement,
f.ad,
g.creative,
h.operating_system,
i."browser/platform" as browser,
a."browser/platform_version" as browser_version,
a."designated_market_area_(dma)" as dma,
a.referrer_url,
COUNT(event_time) as impressions
from
pntheon_clients.pntheon_dcm_impression a
inner join
(select advertiser_id, advertiser from pntheon_clients.pntheon_dcm_match_tables_advertisers group by advertiser_id, advertiser) b
on
a.advertiser_id = b.advertiser_id 
inner join
(select campaign_id, campaign from pntheon_clients.pntheon_dcm_match_tables_campaigns group by campaign_id, campaign) c
on
a.campaign_id = c.campaign_id
inner join
(select "site_id_(dcm)", "site_(dcm)" from pntheon_clients.pntheon_dcm_match_tables_sites group by "site_id_(dcm)", "site_(dcm)") d
on
a."site_id_(dcm)" = d."site_id_(dcm)"
inner join
(select placement, placement_id from pntheon_clients.pntheon_dcm_match_tables_placements group by placement, placement_id) e
on
a.placement_id = e.placement_id
inner join
(select ad, ad_id from pntheon_clients.pntheon_dcm_match_tables_ads group by ad, ad_id) f
on
a.ad_id = f.ad_id
left join
(select rendering_id, creative from pntheon_clients.pntheon_dcm_match_tables_creatives group by rendering_id, creative) g
on
a.rendering_id = g.rendering_id
left join
(select operating_system_id, operating_system from pntheon_clients.pntheon_dcm_match_tables_operating_systems group by operating_system_id, operating_system) h
on
a.operating_system_id = h.operating_system_id
left join
(select "browser/platform_id", "browser/platform" from pntheon_clients.pntheon_dcm_match_tables_browsers group by "browser/platform_id", "browser/platform") i
on
a."browser/platform_id" = i."browser/platform_id"
where
a.advertiser_id = '10562933'
group by
date_format(from_unixtime(cast(event_time as bigint)/1000000, 'America/New_York'), '%Y-%m-%d'),
b.advertiser,
c.campaign,
d."site_(dcm)",
e.placement,
f.ad,
g.creative,
h.operating_system,
i."browser/platform",
a."browser/platform_version",
a."designated_market_area_(dma)",
a.referrer_url
;

select
site,
min("date"),
max("date"),
sum(impressions)
from obefitness.dcm_basic_performance_report_impressions
where 
campaign like '%_PHASE2_%'
and
"date" >= '2021-03-14'
and
"date" <= '2021-03-21'
group by
site
;


select
distinct(referrer_url)
from
pntheon_clients.pntheon_dcm_impression
where
"site_id_(dcm)" = '6555407'
;




