CREATE OR REPLACE VIEW dune_user_generated.nft_wallet_funders AS

WITH looksrare_addresses as (
SELECT unnest(array[buyer,seller]) as wallet
from dune_user_generated.sohwak_os_nft_trades_looksrare
where block_time >= current_date - '30 day'::interval
)

, x2y2_addresses as (
SELECT unnest(array[buyer,seller]) as wallet
from dune_user_generated.kqian_os_nft_trades_x2y2_all
where block_time >= current_date - '30 day'::interval
)

, all_addresses as (
select distinct wallet
from (
    select * from looksrare_addresses
    union
    select * from x2y2_addresses
    ) x
)

, aggregator_addresses as (
    select agg_address::bytea as agg_address
    from dune_user_generated.nft_aggregator
)

, cex_addresses as (
    select address::bytea as cex_address
    from dune_user_generated.cex_addresses
)

, contracts as (
    select address::bytea as contract_address
    from ethereum.contracts
)

, funders as (
    select
        wallet
        ,funder
    from (
        select
        wallet
        ,tx."from" as funder
        ,row_number() over (partition by tx."to" order by block_time asc) as ordering
        from all_addresses
        inner join ethereum."transactions" tx
        ON wallet = tx."to"
        left join aggregator_addresses agg
        ON agg_address = tx."from"
        left join cex_addresses cex
        ON cex_address = tx."from"
        left join contracts con
        on contract_address = tx."from"
        where agg_address is null and cex_address is null
    ) f
    where ordering = 1
)

select * from funders
