## Database: `adx`

### Purpose

The `adx` database contains device usage data ETL'd from **Azure Data Explorere** (**ADX**) used for eproting device usage like average pumping hours.

## Tables

### `adx.device_daily_usage`

- One row per device for avearge pumping hours per day
- Tracks daily avgerage pumping hours per each device

#### Key Identifiers

- `deviceId` - Unique device identifier. Links to `amt.account_devices.id`
- `timestamp` - daily Date for which the device was used
- `energyConsumptionKwh` - daily enegery consumption by device in KwH
- `pumpruntimehrs` - Number of hours the devcie was running per day
- `Source` - Indicate whether the device is a `Battery` or a `Direct`
- `Variant` - Device Variant
- `FwVer` - Firmware version of the device

#### Notes

- `timestamp` is a daily grain — one row per device per day
- `pumpruntimehrs = 0` may indicate the device was inactive that day or data was not transmitted
- `Source = 'Battery'` vs `Source = 'Direct'` affects energy consumption interpretation
- For average pumping hours analysis, aggregate `pumpruntimehrs` over the desired date range

#### Canonical Base Query

with
device_daily_usage_cte as (
SELECT distinct deviceId,
timestamp,
energyConsumptionKwh,
pumpruntimehrs,
Source,
FwVer,
Variant
FROM adx.device_daily_usage
)
SELECT \*
from device_daily_usage_cte
