"""
Regenerate the holidays seed. Run manually when:
  - the `holidays` package releases an update (e.g. a new federal
    holiday is legislated), or
  - the covered year range needs extending.

    uv pip install --upgrade holidays
    uv run python scripts/generate_holidays_seed.py
    cd dbt/flights && dbt seed
"""

import csv
import holidays

# Wide range - a seeds cost nothing extra, and this avoids needing
# to regenerate it if the ingestion windows grows

us_holidays = holidays.US(years=range(1987, 2051))

with open("dbt/flights/seeds/holidays.csv", "w", newline="") as f:
    writer = csv.writer(f)
    # header
    writer.writerow(["holiday_date", "holiday_name"])
    # content
    for date, name in sorted(us_holidays.items()):
        writer.writerow([date, name])
