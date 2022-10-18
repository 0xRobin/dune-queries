CREATE OR REPLACE VIEW dune_user_generated.xrob_x2y2_wash AS
(
-- ####################
-- 1. SOURCE TABLES
-- ####################
with trades_erc721 as (
    select *
    from dune_user_generated.kqian_x2y2_erc721
    where block_time >= date_trunc('day',current_date) - '30 days'::interval
    -- and block_time < date_trunc('day',current_date)
)

, trades_all as(
    select *
    from dune_user_generated.kqian_os_nft_trades_x2y2_all
    where block_time >= date_trunc('day',current_date) - '30 days'::interval
    -- and block_time < date_trunc('day',current_date)
)

, opensea_trades as (
    select *
    from nft.trades
    where platform = 'OpenSea'
    and block_time >= date_trunc('day',current_date) - '30 days'::interval
    -- and block_time < date_trunc('day',current_date)
)

, aggregator_addresses as (
    select agg_address::bytea as agg_address
    from dune_user_generated.nft_aggregator
)

, cex_addresses as (
    select address::bytea as cex_address
    from dune_user_generated.cex_addresses
)

, funders as (
    select wallet, funder
    from dune_user_generated.nft_wallet_funders
)

, royalty_settings as (
    select
        collection
        ,fee
    from (
        select
            *
            ,row_number() over (partition by collection order by evt_block_time desc) as ordering
        from looksrare."RoyaltyFeeRegistry_evt_RoyaltyFeeUpdate"
    ) foo
    where ordering = 1
)
-- ####################
-- 2. FILTER LISTS
-- ####################

-- multiple trades
, mt_filter as (
    select *
    , 'mt' as filter
    from (
        select date_trunc('day', block_time) as day
          ,nft_contract_address
          ,nft_token_id
          ,count(1) as num_sales
        from trades_erc721
        group by 1,2,3
    ) trade_count
    where true AND num_sales >= 3
)

-- seller/buyer
-- multiple transactions between same seller/buyer combo on the same day
,sb_filter as (
    select
    *
    ,'sb' as filter
    from (
        select date_trunc('day', block_time) as day
          , case when seller > buyer then seller else buyer end as address1
          , case when seller > buyer then buyer else seller end as address2
          , count(distinct tx_hash) as num_sales
        from trades_all
        left join aggregator_addresses on agg_address = buyer
        where agg_address is null
        group by 1,2,3
    ) foo
    where true and num_sales >= 3
)


-- low volume
-- collections with volume lower then 100 USD on OpenSea
-- This will overfit on small collections but won't impact overall volume stats much.
, lv_filter as (
    select *
    , 'lv' as filter
    from (
        select
        nft_address
        ,sum(usd_amount) as os_vol
        from (
            select distinct nft_contract_address as nft_address
            from trades_all
        ) collections
        left join opensea_trades
        ON nft_address = nft_contract_address
        group by 1
    ) foo
    where true and os_vol < 100
)

-- high prices
-- filters out high prices on 0% royalty transactions
-- high is defined as 10x above the max collection trade on OpenSea in USD (within the timeframe)
, hp_filter as (
    select *
    , 'hp' as filter
    from (
        select
        nft_address
        ,10 * max(usd_amount) as highprice_cutoff
        from (
            select distinct nft_contract_address as nft_address
            from trades_all
            inner join royalty_settings
            ON nft_contract_address = collection and fee = 0
        ) collections
        left join opensea_trades
        ON nft_address = nft_contract_address
        group by 1
    ) foo
    where true
)

-- wallet funder
-- filters out
, wf_filter as (
    select distinct
        buyer
        ,seller
        ,'wf' as filter
    from trades_all
    left join funders f1
      on f1.wallet = buyer
    left join funders f2
      on f2.wallet = seller
    where true -- wf_filter
      and f1.funder = f2.funder
      or (f1.funder = seller or f2.funder = buyer)
)

-- manual filters
-- DISABLED
-- these manual entries are blacklisted
, mn_filter as (
    select
      nft_contract_address::bytea as nft_contract_address
      ,user_address::bytea as user_address
      ,token_id as token_id
      ,'mn' as filter
    from (VALUES
            ('\xccb893eFB3ECE7816EABE0ce73Fb4767d69CD036',null,null),
            ('\x1dfe7ca09e99d10835bf73044a23b73fc20623df',null,null),
            ('\x4E1f41613c9084FdB9E34E11fAE9412427480e56',null,null),
            ('\x28462739c3eb65a571bf92689e8257c806bd275d',null,null),
            (null,'\x558faecdb2405895267d47133e65afc1696aeef9',null),
            (null,'\x23aa2e12fce924769d58fa0ab63fc59a2667582d',null),
            (null,'\x558faecdb2405895267d47133e65afc1696aeef9',null),
            (null,'\x928e87ae4af2883b342962b317a1b98cd401fcc6',null),
            (null,'\x73d604e0493a09dca4b705f6205f4a7bc64f813e',null),
            (null,'\x23aa2e12fce924769d58fa0ab63fc59a2667582d',null),
            (null,'\x558faecdb2405895267d47133e65afc1696aeef9',null),
            (null,'\xcce2c348c923ad05d7ff6b2853d69b72be0e2e6d',null),
            (null,'\xec12eb821664b2c94eba4a738aac93189629e0a6',null),
            (null,'\x0905f5c7e968dae9e539342dab7967c19dc6251a',null),
            (null,'\x7e38c7bfb6e26bd00e46333b066f3ef9da9a0ab5',null),
            (null,'\xadcecf110c7f75729da078bad3915fd95c00f932',null),
            (null,null,'42478833275053992748391598026896626381208592366362073031419798430398276302010'),
            (null,null,'110715061140428961420186765642187791424560268085393978504072574013791734540618'),
            (null,null,'110715061140428961420186765642187791424560268085393978504072574013791734540618'),
            (null,null,'20196220359150090438441739350392470717747863933175327108295111838711609521468')
      ) AS temp_table (nft_contract_address, user_address,token_id)
      where false -- mn_filter
)

-- ####################
-- 3. Trades Enrichment
-- ####################

, trades as (
    select date_trunc('day',a.block_time) as day
          ,a.block_time
          ,a.platform
          ,a.nft_contract_address
          ,a.nft_token_id
          ,a.tx_hash
          ,erc20.symbol as currency
          ,a.original_amount / 10^erc20.decimals AS amount
          ,p.price as usd_price
          ,a.original_amount / 10^erc20.decimals * p.price AS usd_amount
          ,a.buyer
          ,a.seller
        from trades_all a
        left join prices."usd" p
            on p.minute >= date_trunc('day',current_date) - '30 days'::interval
             and p.minute = date_trunc('minute',a.block_time)
             and p.contract_address = a.currency_contract
        left join erc20.tokens erc20 on a.currency_contract = erc20.contract_address
)

, filtered_trades as (
    select t.*
        , case when mt.filter is not null then true else false end as mt_filter
        , case when sb.filter is not null then true else false end as sb_filter
        , case when lv.filter is not null then true else false end as lv_filter
        , case when hp.filter is not null then true else false end as hp_filter
        , case when wf.filter is not null then true else false end as wf_filter
        , case when mn.filter is not null then true else false end as mn_filter
        , coalesce(array_remove(array[mt.filter,sb.filter,lv.filter,hp.filter,wf.filter,mn.filter],null),'{}') as wash_filters
    from trades t
    -- multiple trades
    left join mt_filter mt
        ON mt.day = t.day
          AND mt.nft_contract_address= t.nft_contract_address
          AND mt.nft_token_id = t.nft_token_id
    -- seller/buyer combo
    left join sb_filter sb
        ON sb.day = t.day
          AND ((t.buyer = sb.address1 and t.seller = sb.address2)
            OR (t.seller = sb.address1 and t.buyer = sb.address2))
    -- low volume
    left join lv_filter lv
        ON lv.nft_address = t.nft_contract_address
    -- high price
    left join hp_filter hp
        ON t.nft_contract_address = hp.nft_address
          AND t.usd_amount > hp.highprice_cutoff
    -- wallet funder
    left join wf_filter wf
        ON wf.buyer = t.buyer and wf.seller = t.seller
    -- manual
    left join mn_filter mn
        ON t.nft_contract_address = mn.nft_contract_address
         OR t.buyer = mn.user_address
         OR t.seller = mn.user_address
         OR t.nft_token_id = mn.token_id
)

select *
,case when cardinality(wash_filters) > 0 then true else false end as any_filter
from filtered_trades
)
