-- noinspection SqlNoDataSourceInspectionForFile
-- push notification delivery stream from push notification delivery topic
create stream pn_delivery_stream (
    subscriber_id bigint,
    delivered_time varchar,
    source varchar,
    source_id bigint,
    website_id bigint
) with (
    kafka_topic = 'pushowl.entity.pn_delivery_avro',
    value_format = 'avro',
    timestamp = 'delivered_time',
    timestamp_format = 'yyyy-MM-dd HH:mm:ss.SSSSSS+0000'
);

-- create delivery stream partitioned by source_id
create stream pn_delivery_by_source_id as
select
    *
from
    pn_delivery_stream partition by source_id;

-- campaign impression report grouped by campaign_id
create table campaign_delivery_report as
select
    count(*) as count,
    source_id
from
    pn_delivery_by_source_id
where
    source = 'campaign'
group by
    source_id emit changes;

-- campaign impression report windowed
create table campaign_delivery_report_windowed as
select
    count(*) as count,
    source_id
from
    pn_delivery_by_source_id WINDOW TUMBLING (size 15 minutes)
where
    source = 'campaign'
group by
    source_id emit changes;