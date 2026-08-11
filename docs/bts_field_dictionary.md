# BTS Reporting Carrier On-Time Performance — Field Dictionary

## Background

The data contained in the compressed file has been extracted from the Reporting Carrier On-Time Performance (1987–present) data table of the "On-Time" database from the TranStats data library. The time period is indicated in the name of the compressed file; for example, `XXX_XXXXX_2001_1` contains data for the first month of 2001.

## Record layout

Fields in the order they appear on the records.

### Flight identifiers & date

| Field | Description |
|---|---|
| Year | Year |
| Quarter | Quarter (1–4) |
| Month | Month |
| DayofMonth | Day of Month |
| DayOfWeek | Day of Week |
| FlightDate | Flight Date (yyyymmdd) |
| Reporting_Airline | Unique Carrier Code. When the same code has been used by multiple carriers, a numeric suffix is used for earlier users, e.g. PA, PA(1), PA(2). Use this field for analysis across a range of years. |
| DOT_ID_Reporting_Airline | Identification number assigned by US DOT to identify a unique airline (carrier). A unique airline is defined as one holding and reporting under the same DOT certificate regardless of Code, Name, or holding company/corporation. |
| IATA_CODE_Reporting_Airline | Code assigned by IATA, commonly used to identify a carrier. As the same code may have been assigned to different carriers over time, it is not always unique — use the Unique Carrier Code for analysis. |
| Tail_Number | Tail Number |
| Flight_Number_Reporting_Airline | Flight Number |

### Origin airport

| Field | Description |
|---|---|
| OriginAirportID | Origin Airport ID. Identification number assigned by US DOT to identify a unique airport. Use for analysis across a range of years, since airport codes can change or be reused. |
| OriginAirportSeqID | Origin Airport Sequence ID — identifies a unique airport at a given point in time. Airport attributes (name, coordinates) may change over time. |
| OriginCityMarketID | City Market ID assigned by US DOT to identify a city market. Use to consolidate airports serving the same city market. |
| Origin | Origin Airport (code) |
| OriginCityName | Origin Airport, City Name |
| OriginState | Origin Airport, State Code |
| OriginStateFips | Origin Airport, State FIPS |
| OriginStateName | Origin Airport, State Name |
| OriginWac | Origin Airport, World Area Code |

### Destination airport

| Field | Description |
|---|---|
| DestAirportID | Destination Airport ID. Same notes as OriginAirportID. |
| DestAirportSeqID | Destination Airport Sequence ID. Same notes as OriginAirportSeqID. |
| DestCityMarketID | Destination Airport, City Market ID. |
| Dest | Destination Airport (code) |
| DestCityName | Destination Airport, City Name |
| DestState | Destination Airport, State Code |
| DestStateFips | Destination Airport, State FIPS |
| DestStateName | Destination Airport, State Name |
| DestWac | Destination Airport, World Area Code |

### Departure performance

| Field | Description |
|---|---|
| CRSDepTime | Scheduled (CRS) Departure Time (local time: hhmm) |
| DepTime | Actual Departure Time (local time: hhmm) |
| DepDelay | Difference in minutes between scheduled and actual departure time. Early departures show negative numbers. |
| DepDelayMinutes | Same as DepDelay, but early departures set to 0. |
| DepDel15 | Departure Delay Indicator, 15 minutes or more (1 = Yes) |
| DepartureDelayGroups | Departure delay intervals, every 15 minutes, from `<-15` to `>180` |
| DepTimeBlk | CRS Departure Time Block, hourly intervals |
| TaxiOut | Taxi Out Time, in minutes |
| WheelsOff | Wheels Off Time (local time: hhmm) |

### Arrival performance

| Field | Description |
|---|---|
| WheelsOn | Wheels On Time (local time: hhmm) |
| TaxiIn | Taxi In Time, in minutes |
| CRSArrTime | Scheduled (CRS) Arrival Time (local time: hhmm) |
| ArrTime | Actual Arrival Time (local time: hhmm) |
| ArrDelay | Difference in minutes between scheduled and actual arrival time. Early arrivals show negative numbers. |
| ArrDelayMinutes | Same as ArrDelay, but early arrivals set to 0. |
| ArrDel15 | Arrival Delay Indicator, 15 minutes or more (1 = Yes) |
| ArrivalDelayGroups | Arrival delay intervals, every 15 minutes, from `<-15` to `>180` |
| ArrTimeBlk | CRS Arrival Time Block, hourly intervals |

### Cancellations & diversions (summary)

| Field | Description |
|---|---|
| Cancelled | Cancelled Flight Indicator (1 = Yes) |
| CancellationCode | Reason for cancellation |
| Diverted | Diverted Flight Indicator (1 = Yes) |

### Flight duration & distance

| Field | Description |
|---|---|
| CRSElapsedTime | Scheduled (CRS) Elapsed Time of Flight, in minutes |
| ActualElapsedTime | Actual Elapsed Time of Flight, in minutes |
| AirTime | Flight Time, in minutes |
| Flights | Number of Flights (usually 1 per row) |
| Distance | Distance between airports, in miles |
| DistanceGroup | Distance intervals, every 250 miles, for flight segment |

### Delay cause breakdown

*Only populated when a flight was delayed ≥15 minutes, per BTS methodology — otherwise null.*

| Field | Description |
|---|---|
| CarrierDelay | Carrier Delay, in minutes |
| WeatherDelay | Weather Delay, in minutes |
| NASDelay | National Air System Delay, in minutes |
| SecurityDelay | Security Delay, in minutes |
| LateAircraftDelay | Late Aircraft Delay, in minutes |

### Gate return / cancelled flight ground time

| Field | Description |
|---|---|
| FirstDepTime | First Gate Departure Time at Origin Airport |
| TotalAddGTime | Total Ground Time Away from Gate for Gate Return or Cancelled Flight |
| LongestAddGTime | Longest Time Away from Gate for Gate Return or Cancelled Flight |

### Diversion detail (summary)

| Field | Description |
|---|---|
| DivAirportLandings | Number of Diverted Airport Landings |
| DivReachedDest | Diverted Flight Reaching Scheduled Destination Indicator (1 = Yes) |
| DivActualElapsedTime | Elapsed Time of Diverted Flight Reaching Scheduled Destination, in minutes. `ActualElapsedTime` remains NULL for all diverted flights. |
| DivArrDelay | Difference in minutes between scheduled and actual arrival time for a diverted flight reaching scheduled destination. `ArrDelay` remains NULL for all diverted flights. |
| DivDistance | Distance between scheduled destination and final diverted airport, in miles. Value is 0 for a diverted flight reaching scheduled destination. |

### Diversion detail — per diverted airport (repeats 5×: Div1–Div5)

Each diverted airport gets its own block of 8 fields. Shown once below for `Div1`; the same fields repeat identically for `Div2` through `Div5`.

| Field | Description |
|---|---|
| Div1Airport | Diverted Airport Code |
| Div1AirportID | Airport ID of the diverted airport — unique key for an airport |
| Div1AirportSeqID | Airport Sequence ID of the diverted airport — unique key for time-specific airport info |
| Div1WheelsOn | Wheels On Time (local time: hhmm) at the diverted airport |
| Div1TotalGTime | Total Ground Time Away from Gate at the diverted airport |
| Div1LongestGTime | Longest Ground Time Away from Gate at the diverted airport |
| Div1WheelsOff | Wheels Off Time (local time: hhmm) at the diverted airport |
| Div1TailNum | Aircraft Tail Number at the diverted airport |

> Applies identically to `Div2*`, `Div3*`, `Div4*`, `Div5*`.