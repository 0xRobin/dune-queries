WITH buys as (
SELECT
"buyer" as "collector",
sum("price") as "buys",
count(*) as "trades"
FROM dune_user_generated.archipelago_trades
GROUP BY "buyer"
),

sells as (
SELECT
"seller" as "collector",
sum("price") as "sells",
count(*) as "trades"
FROM dune_user_generated.archipelago_trades
GROUP BY "seller"
),

data as (
SELECT
coalesce(b."collector", s."collector") as "collector",
coalesce(b."buys",0) as "buy_volume",
coalesce(s."sells",0) as "sell_volume",
coalesce(b."buys",0) + coalesce(s."sells",0) as "total_volume",
coalesce(b."trades",0) + coalesce(s."trades",0) as "trades"
FROM buys b
FULL JOIN sells s
ON b."collector" = s."collector"
)


SELECT
CONCAT('<a href="https://archipelago.art/address/0x',substring("collector"::text from 3),
'" target="_blank" >🎨</a>') as "link",
coalesce((labels.get("collector",'ens name reverse'))[1],CONCAT('0x',substring("collector"::text from 3 for 4),'...',substring("collector"::text from 40))) as "collector",
"buy_volume",
"sell_volume",
"total_volume",
"trades"
FROM data
ORDER BY "buy_volume" DESC
LIMIT 30
