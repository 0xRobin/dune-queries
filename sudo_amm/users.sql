WITH
all_transactions as (
SELECT block_time, "from" as user_address, hash FROM ethereum.transactions
where success and "to" = '\x2b2e8cda09bba9660dca5cb6233787738ad68329' --sudoAMM router
)

, daily_new_user_addresss as (
SELECT join_date, count(*) as new_user_addresss
FROM (
    SELECT user_address, date_trunc('day', min(block_time)) as join_date FROM all_transactions GROUP BY user_address
    ) foo
GROUP BY join_date
)

, daily_unique_user_addresss as (
    SELECT day, count(distinct user_address) as unique_user_addresss
FROM (
    SELECT user_address, date_trunc('day',block_time) as day FROM all_transactions
    ) foo
GROUP BY day
)

, days AS (
    SELECT generate_series('2022-05-25'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    )

SELECT d.day
, coalesce(new_user_addresss, 0) as "new users"
, coalesce(unique_user_addresss, 0) as unique_user_addresss
, avg(coalesce(unique_user_addresss, 0)) over (order by d.day rows between 6 preceding and current row) as "daily active users [7d]"
, sum(coalesce(new_user_addresss,0)) over (order by d.day) as "total users"
FROM days d
LEFT JOIN daily_new_user_addresss n ON d.day = n.join_date
LEFT JOIN daily_unique_user_addresss u ON d.day = u.day
order by d.day DESC