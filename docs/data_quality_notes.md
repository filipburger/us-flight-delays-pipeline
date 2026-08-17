# Data Quality Notes

Findings from investigating the BTS On-Time Reporting Carrier dataset before
and during pipeline development. Each was verified against the data rather
than assumed.

---

## 1. Schema stability (2015–2026)

**Question:** does the column set change across the ingestion window, requiring
per-era reconciliation in staging?

**Method:** downloaded January of 2015, 2018, 2020, 2022, 2024 and 2026, and
diffed the column name sets between consecutive samples.

**Result:** 110 columns throughout, with no additions, removals or renames.

**Consequence:** no schema-drift handling is needed. Instead, ingestion pins
explicit dtypes and raises on any deviation from the expected column set — so
if BTS ever does change the format, the pipeline stops loudly rather than
silently writing mismatched parquet.

Note that three field groups do not exist in earlier data, per BTS
documentation. All predate the 2018 ingestion window, so they don't affect
this pipeline, but they'd matter if the range were extended:

| Field group | Available from |
|---|---|
| Cause of Delay (`CarrierDelay`, `WeatherDelay`, …) | June 2003 |
| Gate Return Information (`FirstDepTime`, `TotalAddGTime`, …) | October 2008 |
| Diverted Airport Information (`Div1*`–`Div5*`) | October 2008 |

---

## 2. Codeshare duplication — investigated, not present

**Question:** since 1 January 2018, [14 CFR §234.4(k)](https://www.ecfr.gov/current/title-14/chapter-II/subchapter-A/part-234)
requires a marketing carrier to separately file on-time performance for
flights operated by a code-share partner. Does the same physical flight
therefore appear as multiple rows under different carrier codes?

**Method:** grouped on a physical-flight fingerprint and looked for groups
containing more than one row.

Two earlier attempts were wrong and worth recording:

- Grouping on `(FlightDate, Origin, Dest)` false-positives on any route flown
  more than once a day.
- Adding `CRSDepTime` still false-positives: multiple unrelated carriers
  schedule departures in the same popular slot. An initial run flagged 3,439
  candidate groups, but inspection showed cases like `LAS→SEA` at 06:00 with
  four distinct carriers — competitive scheduling, not duplicate filings.

`Tail_Number` is the disambiguator: two rows describing the same physical
flight must share an aircraft. Final key:
(FlightDate, Tail_Number, Origin, Dest, CRSDepTime)

**Result: 0 duplicate filings across 541,978 physical flights (January 2024).**

**Explanation:** the regional carriers that would otherwise produce this
pattern — SkyWest (`OO`), Envoy (`MQ`), Republic (`YX`), Endeavor (`9E`),
PSA (`OH`) — already meet BTS's independent reporting threshold and file under
their own carrier codes. §234.4(k) exists to close a gap where a flight would
otherwise go unreported; that gap rarely opens in this dataset. BTS also
publishes the marketing-carrier view as a *separate* file series
(`On_Time_Marketing_Carrier_On_Time_Performance_Beginning_January_2018_*`)
rather than mixing both into one table.

**Consequences:**

- No deduplication step is required in staging.
- `Reporting_Airline` can be treated as the **operating** carrier, so
  delay-probability-by-carrier measures operational reliability rather than
  which brand sold the ticket.

---

## 3. Provenance vs. source columns

The Hive partition path encodes which BTS file a row came from. BTS also
publishes `Year` and `Month` columns describing the flight itself. These
initially collided in BigQuery, surfacing as `Year_1` / `Month_1`.

The collision was resolved by renaming the partition keys to `source_year` /
`source_month` rather than by dropping either set of columns, because they
answer different questions:

| Column | Meaning |
|---|---|
| `source_year`, `source_month` | Which monthly file this row was published in |
| `Year`, `Month` | What BTS reports about the flight |
| `FlightDate` | The actual event date — the source of truth for analysis |

Under normal conditions all three agree. Keeping them separate makes any
disagreement detectable — for example a late-reported flight appearing in a
subsequent month's file. All analytical date logic derives from `FlightDate`;
the partition keys are provenance only.

A dbt test asserts the assumption holds:

```sql
select * from {{ ref('stg_flights') }}
where extract(month from flight_date) != source_month
```

---

## 4. Null patterns

Roughly 40% of columns are heavily null. Almost none of this is a data quality
problem — the nulls are structural.

| Column group | Null rate | Reason |
|---|---|---|
| `Div1*`–`Div5*` (40 cols) | 96–100% | Diversions are rare; `Div2`–`Div5` require repeated rerouting of a single flight |
| `CancellationCode` | 96% | Only populated for cancelled flights |
| Delay causes (`CarrierDelay` etc.) | 77% | BTS only attributes cause when a flight is delayed ≥15 minutes |
| `ActualElapsedTime`, `ArrTime`, `TaxiIn` … | ~4% | Cancelled and diverted flights never departed or arrived |
| `Unnamed: 109` | 100% | Not data — a phantom column from a trailing comma on every source row |

Only `Unnamed: 109` is dropped at ingestion, as it isn't data at all. Every
other sparse column is preserved; the delay-cause columns in particular are
central to the analysis.

A diversion is a **single scheduled flight** rerouted mid-air, not a
multi-segment itinerary — connecting flights appear as separate rows with
their own flight numbers. `Origin`/`Dest` remain as originally scheduled;
`Div1Airport` onward record where the aircraft actually landed. The 40 wide
columns are collapsed in dbt to a `was_diverted` flag plus a `final_destination`,
with full detail unpivoted into a separate normalised table.

---

## 5. Lookup tables — what's used and what isn't

BTS publishes 19 lookup tables resolving coded columns, all reachable from the
[field selector page](https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr).
All are ingested (they total a few hundred KB), but only a subset is modelled.

### Used

| Table | Joins on | Provides |
|---|---|---|
| `l_unique_carriers` | `Reporting_Airline` | Carrier name |
| `l_cancellation` | `CancellationCode` | Cancellation cause (Carrier / Weather / NAS / Security) |
| `l_ontime_delay_groups` | `DepartureDelayGroups`, `ArrivalDelayGroups` | 15-minute delay band labels |
| `l_city_market_id` | `OriginCityMarketID`, `DestCityMarketID` | Metro-area grouping, so JFK/LGA/EWR can be analysed as one New York market |

**Join on `Reporting_Airline`, not `IATA_CODE_Reporting_Airline`.** BTS states
the IATA code is not always unique, as the same code has been reassigned to
different carriers over time. `Reporting_Airline` is the unique carrier code
and is the field BTS recommends for analysis across a range of years.
`l_unique_carriers` is keyed on it — visible in the data, whose codes (`02Q`,
`05Q`, `06Q`) are three characters, not two-character IATA codes.

### Not modelled

| Table | Reason |
|---|---|
| `l_airline_id` | Same 1,778 carriers as `l_unique_carriers`, keyed on `DOT_ID_Reporting_Airline` with the code embedded in the description string (`"Mackey International Inc.: MAC"`). The DOT ID is the more durable key — a certificate number is never reused — so it is retained on the fact table, but the label is taken from `l_unique_carriers` to avoid string parsing. |
| `l_carrier_history` | Validity ranges are embedded in the description (`"Tradewind Aviation (2006 - 2023)"`) rather than being proper date columns. Superseded by `l_unique_carriers`. |
| `l_months`, `l_weekdays`, `l_quarters` | Derivable from `FlightDate` in `dim_date`, with more flexibility over naming and locale |
| `l_yesno_resp` | A 0/1 → boolean cast |
| `l_deparrblk` | Only reformats `0700-0759` as `7:00AM to 7:59AM`. The underlying `DepTimeBlk` column is retained — hour-of-day is a useful analytical dimension — but the label adds nothing. |
| `l_distance_group_250`, `l_diversions` | Small static label sets, hardcoded in dbt if needed |
| `l_state_abr_aviation`, `l_state_fips` | `OriginStateName` / `DestStateName` are already denormalised onto the fact table |
| `l_world_area_codes` | International region grouping, irrelevant for domestic data |

### Access note

BTS obfuscates URL parameters by rotating 13 positions through a combined
digit + uppercase + lowercase alphabet:

0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz

This is **not** standard ROT13 — characters near the end of one range wrap
into the next, so `N→a`, `T→g`, `2→F`. `L_CANCELLATION` encodes as
`Y_PNaPRYYNgVba`, and the on-time table's `gnoyr_VQ=FGJ` decodes to
`table_ID=236`. Because the alphabet has 62 characters rather than 26, the
transform is not self-inverse and encode/decode need separate tables. See
`bts_encode()` / `bts_decode()` in `dags/bts_lookups.py`.