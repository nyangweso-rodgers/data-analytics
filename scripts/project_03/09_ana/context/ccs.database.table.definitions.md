## Database: `ccs`

### Purpose

The `ccs` database contains data from Call Center operations including both
inbound and outbound calls. The platform used for call center activities is
called **Call Center Studio**, hence the abbreviation `ccs`.

## Tables

### `ccs.queue_performance`

One row per queue per day. End-of-day performance summary for each Call Center Queue.

#### Key Identifiers

- `country` - Country the queue operates in i.e., kenya, uganda, civ
- `queue_key` - Unique identifier for the Call Center Queue
- `queue_date` - Date of the performance summary (daily grain)
- `queue_name` - Human readable name of the queue

#### Inbound Call Volume

- `number_of_inbound_calls` - Total number of inbound calls that entered the queue
- `answered` - Boolean flag indicating whether calls in the queue were answered
- `answer_rate` - Percentage of inbound calls that were answered
- `answered_in_sl` - Number of calls answered within the Service Level (SL) threshold
- `abandon` - Number of inbound calls abandoned by the caller before being answered
- `abandon_rate` - Percentage of inbound calls that were abandoned

#### Inbound Wait & Talk Duration

- `wait_duration` - Total wait time across all inbound calls in seconds
- `max_wait_duration` - Maximum wait time experienced by a single caller in seconds
- `avg_wait_duration` - Average wait time per inbound call in seconds
- `answer_speed` - Average speed of answer — time in seconds from call arrival to agent pickup (lower is better)
- `inbound_talk_duration` - Total talk time for inbound calls in seconds
- `avg_inbound_talk_duration` - Average talk time per inbound call in seconds

#### Outbound Call Volume

- `number_of_outbound_calls` - Total number of outbound call attempts from the queue
- `succesful_outbound_calls` - Total number of outbound calls that were successfully connected
- `outbound_answer_rate` - Percentage of outbound call attempts that were successfully connected
- `outbound_local_release` - Number of outbound calls released/ended by the local agent side

#### Outbound Talk Duration

- `outbound_talk_duration` - Total talk time for outbound calls in seconds
- `avg_outbound_talk_duration` - Average talk time per outbound call in seconds

#### Notes

- One row per `queue_key` + `queue_date` — dedup uses `_synced_at DESC`
- All duration columns are in **seconds** — divide by 60 for minutes, 3600 for hours
- `answer_rate` + `abandon_rate` should sum to ~100% — use as a data quality check
- `answered_in_sl` <= `answered` always — use to measure service level compliance
- `number_of_outbound_calls` >= `succesful_outbound_calls` — attempts include unanswered and failed dials
- `outbound_local_release` indicates calls the agent ended — useful for identifying premature disconnections
- Queues with `number_of_inbound_calls = 0` on a given day may indicate the queue was inactive or a sync issue

#### Canonical Base Query

```sql
WITH
queue_performance_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by queue_key, queue_date ORDER BY _synced_at desc) as rnk
        FROM ccs.queue_performance
    ) where rnk = 1
    )
select *
from queue_performance_cte
```

### `ccs.user_performance`

One row per agent per day. End-of-day performance summary for each Call Center
Representative/Agent.

#### Key Identifiers

- `country` - Country the agent operates in i.e., kenya, uganda
- `performance_date` - Date of the performance summary (daily grain)
- `user_email` - Unique identifier for the Call Center Representative/Agent
- `user_name` - Full name of the Agent

#### Call Volume

- `total_calls` - Total number of calls handled by the agent on that day
- `inbound_calls` - Total number of inbound calls received by the agent
- `outbound_attempts` - Total number of outbound call attempts made by the agent
- `outbound_calls` - Total number of outbound calls that were successfully connected
- `missed_calls` - Total number of inbound calls that were not answered by the agent

#### Talk Duration

- `total_talk_duration` - Total talk time across all calls (inbound + outbound) in seconds
- `average_talk_duration` - Average talk time per call across all calls in seconds
- `inbound_total_talk_duration` - Total talk time for inbound calls in seconds
- `inbound_average_talk_duration` - Average talk time per inbound call in seconds
- `outbound_total_talk_duration` - Total talk time for outbound calls in seconds
- `outbound_average_talk_duration` - Average talk time per outbound call in seconds

#### Call Quality & Efficiency

- `call_hold_count` - Number of times calls were placed on hold
- `call_hold_total_duration` - Total duration calls were on hold in seconds
- `answer_speed` - Average speed of answer — time in seconds from call arrival
  to agent pickup (lower is better)
- `aht` - Average Handling Time per call in seconds. Includes talk time + hold
  time + any wrap-up time

#### Notes

- One row per `user_email` per `performance_date` — dedup uses `_synced_at DESC`
- `outbound_attempts` >= `outbound_calls` — attempts include unanswered/failed dials
- All duration columns are in **seconds** — divide by 60 for minutes, 3600 for hours
- `aht` is the primary KPI for agent efficiency — lower values indicate faster resolution
- `answer_speed` is only meaningful for inbound calls
- Agents with `total_calls = 0` on a given day may indicate absence or a sync issue

#### Canonical Base Query

```sql
WITH user_performance_cte AS (
    SELECT *
    FROM (
        SELECT *,
        row_number() OVER (PARTITION BY user_email, performance_date
                           ORDER BY _synced_at DESC) AS rnk
        FROM ccs.user_performance
    ) WHERE rnk = 1
)
SELECT *
FROM user_performance_cte
```
