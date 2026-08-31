select date_day as date_day
from {{ ref('dim_date') }}