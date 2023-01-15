-------------------------------------------- BigQuery Audit  Logs --------------------
/* Query generates the BQ users and Bill per User and per Query */
with
bigquery_users_report as (
                          select resource.labels.project_id	as project_id,
                          protopayload_auditlog.authenticationInfo.principalEmail	as user,
                          protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime as startTime,
                          cast(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime as date) as date,
                          protopayload_auditlog.requestMetadata.callerSuppliedUserAgent	as userAgent,
                          protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query. query as query,
                          protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes/1024/1024/1024/1024*5 as cost
                          from
                          `auditlog.cloudaudit_googleapis_com_data_access_*`  
                          --where protopayload_auditlog.authenticationInfo.principalEmail = "big-query-access@kyosk-prod.iam.gserviceaccount.com";
                          )
select distinct userAgent,user,query, sum(cost) from bigquery_users_report
where date = '2022-11-08'
group by 1,2,3
order by 4 desc