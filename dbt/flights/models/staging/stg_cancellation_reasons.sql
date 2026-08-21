-- Enables cancellation reasoning

select
    Code as cancellation_code,
    Description as cancellation_reason

from {{ source("raw_bts", "l_cancellation") }}
