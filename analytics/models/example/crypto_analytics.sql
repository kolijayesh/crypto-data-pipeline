{{ config(materialized='table') }}

with base_data as (
    -- Ab hum 'main' source se data mang rahe hain
    select * from {{ source('main', 'raw_prices') }}
)

select 
    id,
    symbol,
    current_price,
    market_cap,
    case 
        when current_price > 50000 then 'Bullish'
        else 'Stable/Bearish'
    end as market_sentiment
from base_data