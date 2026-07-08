## Database: `ccs`

### Purpose

The `ccs` database contains data from Call Center operations including both
inbound and outbound calls. The platform used for call center activities is
called **Call Center Studio**, hence the abbreviation `ccs`.

## Tables

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
