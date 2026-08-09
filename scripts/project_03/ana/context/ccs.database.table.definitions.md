## Database: `ccs`

### Purpose

The `ccs` database contains data from Call Center operations including both inbound and outbound calls. The platform used for call center activities is called **Call Center Studio**, hence the abbreviation `ccs`.

## Tables

### 1. `ccs.calls`

One row per call. Contains the full details and metadata for each unique call.
The aggregated metrics in `ccs.queue_performance` and `ccs.user_performance` are derived from this table.

#### Key Identifiers

- `country` - Country the call originated from i.e., kenya, uganda, civ
- `call_key` - Unique identifier for the call (primary dedup key)
- `call_id` - Alternative unique identifier for the call. Same grain as `call_key`

#### Call Classification

- `is_inbound` - Boolean. `true` for inbound calls, `false` for outbound calls
- `is_answered` - Boolean. `true` if the call was answered by an agent
- `is_assigned` - Boolean. `true` if the call was assigned to a specific agent
- `is_abandoned` - Boolean. `true` if the caller hung up before being answered
- `has_voicemail` - Boolean. `true` if the caller left a voicemail
- `black_list` - Boolean. `true` if the caller is on the blacklist
- `status` - Call status: `hangup` or `NULL`
- `disposition` - Final call disposition:
  - `ANSWER` - Call was answered
  - `BUSY` - Line was busy
  - `CANCEL` - Call was cancelled before connecting
  - `NOANSWER` - Call was not answered
  - `CHANUNAVAIL` - Channel unavailable
  - `CONGESTION` - Network congestion
  - `PREDIAL` - Call in pre-dial state
  - `NULL` / `BLANK` - Disposition not recorded

#### Queue & Agent

- `queue_name` - Name of the queue the call was routed through
- `agent_name` - Full name of the agent who handled the call
- `agent_email` - Email of the agent. Links to `ccs.user_performance.user_email`
- `caller_id` - Caller phone/mobile number

#### Dates & Timestamps

- `queue_date` - Date the call entered the queue
- `call_date` - Datetime the call was initiated
- `talk_date` - Datetime the call was answered and talk began
- `hangup_date` - Datetime the call ended

#### Duration Columns (in seconds)

- `wait_duration` - Time in seconds the caller waited before being answered
- `duration` - Total call duration in seconds including talk and hold time
- `hold_duration` - Time in seconds the call was placed on hold
- `voicemail_duration` - Duration of the voicemail left by the caller in seconds

#### Notes

- All duration columns are in **seconds** — divide by 60 for minutes, 3600 for hours
- `is_abandoned = true` and `is_answered = true` should be mutually exclusive — use as a data quality check
- `disposition = 'ANSWER'` aligns with `is_answered = true` — use either but be consistent
- `agent_email` may be NULL for abandoned or unanswered calls where no agent was assigned
- `talk_date` may be NULL if the call was never answered
- `hangup_date` may be NULL if the call record was not fully synced
- For inbound analysis filter `is_inbound = true`, for outbound filter `is_inbound = false`
- This is the most granular table in `ccs` — use `queue_performance` and `user_performance` for aggregated reporting and this table for call-level analysis

#### Canonical Base Query

```sql
    WITH
--------------------- ccs - calls ----------------------------------
calls_cte as (
    select *
    from (
        SELECT *,
        row_number()over(partition by call_key ORDER BY _synced_at desc) as rnk
        FROM ccs.calls
        ) where rnk = 1
    )
select *
from calls_cte
```

### 2. `ccs.queue_performance`

One row per queue per day. End-of-day performance summary for each Call Center Queue.

#### Key Identifiers

- `country` - Country the queue operates in i.e., kenya, uganda
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

### 3. `ccs.user_performance`

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
