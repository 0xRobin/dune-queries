WITH sales AS (
SELECT day, sum(price) as volume, count(*) as trades FROM(

SELECT
date_trunc('day', block_time) AS day,
"price"
FROM dune_user_generated.archipelago_trades
) foo
group by day
)

select d.day,
coalesce(s.volume, 0) as volume,
coalesce(s.trades, 0) as trades,
sum(s.volume) over (order by d.day) as cummulative_volume,
sum(s.trades) over (order by d.day) as cummulative_trades
from
(SELECT generate_series('2022-06-20',NOW(),'1 day') AS day) d
LEFT JOIN sales s
ON d.day = s.day
order by day desc
