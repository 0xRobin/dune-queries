WITH swaps as (
SELECT
    "call_block_time",
    ("swapListEntry"->'maxCost')::bigint as "maxCost",
    CONCAT('\x',substring(("swapListEntry"->'swapInfo'->>'pair')::text from 3))::bytea as "pair",
    array_agg(id::int ORDER BY id ASC) as "nftIds",
    "tx_hash",
    "swapListEntry"
FROM(
SELECT
    jsonb_array_elements("swapList") as "swapListEntry",
    "tx_hash","call_block_time"
FROM    (
    SELECT "swapList","call_tx_hash" as "tx_hash","call_block_time" FROM
    (
        SELECT *
        FROM sudo_amm."LSSVMRouter_call_robustSwapETHForSpecificNFTs"
    ) foo
    WHERE "call_success" = 'true'
        ) foo
    ) foo
    LEFT JOIN LATERAL jsonb_array_elements("swapListEntry"->'swapInfo'->'nftIds') id ON true
    GROUP BY call_block_time, "swapListEntry", "tx_hash"
)

,hashes AS (SELECT DISTINCT "tx_hash" FROM swaps)

,eth_payments AS (
SELECT "tx_hash", "from", "to", sum("value") as "value"
FROM ethereum.traces
WHERE "tx_hash" in (select * from hashes) AND "value" > 0
GROUP BY "tx_hash", "from", "to"
)

,pairs AS (
SELECT "nft_contract", "pair", "asset_recipient", "fee_recipient" FROM
    (SELECT distinct "poolAddress" FROM sudo_amm."LSSVMPairFactory_evt_NewPair") evt
    LEFT JOIN (
   SELECT "_nft" as nft_contract,
        "output_pair" as "pair",
        CASE WHEN "_assetRecipient" = '\x0000000000000000000000000000000000000000' THEN "output_pair" ELSE "_assetRecipient" END as "asset_recipient",
        "contract_address" as "fee_recipient"
        FROM sudo_amm."LSSVMPairFactory_call_createPairETH" WHERE "call_success" = 'true'
    ) call
    ON evt."poolAddress" = call."pair"
),

transfers AS (
SELECT t."evt_tx_hash", t."from", t."contract_address", array_agg(t."tokenId"::int ORDER BY t."tokenId" ASC) as "transferIds"
FROM erc721."ERC721_evt_Transfer" t
WHERE t."evt_tx_hash" in (select * from hashes)
GROUP BY t."evt_tx_hash", t."contract_address", t."from"
)

,all_swaps AS (
SELECT s.*,
    t."transferIds",
    array(select unnest(s."nftIds") except select unnest(t."transferIds")) as "unfilled",
    array(select unnest(s."nftIds") intersect select unnest(t."transferIds")) as "filled"
FROM swaps s
LEFT JOIN transfers t
ON s.tx_hash = t.evt_tx_hash and s.pair = t."from"
)

,full_filled_swaps AS (
SELECT * FROM all_swaps
WHERE cardinality(unfilled) = 0
)

,partial_filled_swaps AS (
SELECT * FROM all_swaps
WHERE cardinality(unfilled) > 0
)

-- SELECT s.*, t.value FROM swaps s LEFT JOIN eth_transactions t ON s.tx_hash  = t.hash
select * FROM full_filled_swaps


