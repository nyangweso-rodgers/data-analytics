----------------------------- Test Script -------------------
----------------------------- Products & Tech --------------
with
jira_issues as (
                SELECT distinct date(CREATED) as CREATED,
                date(UPDATED) as UPDATED,
                date(DUE_DATE) as DUE_DATE,
                date(RESOLUTION_DATE) as RESOLUTION_DATE,
                ISSUE_KEY,
                ISSUE_TYPE_NAME,
                ISSUE_STATUS_NAME


                FROM `kyosk-prod.Jira.Issues` 
                --where ISSUE_TYPE_NAME in ('Incident')
                order by 1 desc
                )
select * from jira_issues
where ISSUE_KEY in ('KBI-362')