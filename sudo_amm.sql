WITH swaps as (
SELECT
    "call_block_time",
    t."from",
    "call_tx_hash" as "tx_hash"
FROM sudo_amm."LSSVMRouter_call_robustSwapETHForSpecificNFTs" s
LEFT JOIN (SELECT * FROM ethereum.transactions WHERE "to" = '\x2B2e8cDA09bBA9660dCA5cB6233787738Ad68329') t
ON t."hash" = s."call_tx_hash"
)

,eth_transfers AS (
SELECT t."tx_hash", t."from", t."to", sum(t."value") as "value"
FROM ethereum.traces t
INNER JOIN swaps s
ON s."tx_hash" = t."tx_hash"
WHERE "tx_success" AND "success" AND "call_type" = 'call'
GROUP BY t."tx_hash", t."from", t."to"
)

,pairs AS (
SELECT "_nft" as "nft_contract",
    "output_pair" as "pair",
    (CASE WHEN "_assetRecipient" = '\x0000000000000000000000000000000000000000' THEN "output_pair" ELSE "_assetRecipient" END) as "asset_recipient",
    "contract_address" as "fee_recipient"
 FROM
    sudo_amm."LSSVMPairFactory_call_createPairETH" call
    WHERE "call_success" = 'true'
)

,eth_pool_transfers  AS (
SELECT "tx_hash", "pair",
    coalesce("payment",0) - coalesce("refund", 0) as "amount",
    coalesce("fees", 0) as "fees"
FROM (
    SELECT "tx_hash", "pair",
        sum("value") filter (WHERE "to" = "pair") as "payment",
        sum("value") filter (WHERE "from" = "pair" AND "to" = "fee_recipient") as "fees",
        sum("value") filter (WHERE "from" = "pair" AND "to" <> "fee_recipient" AND "to" <> "asset_recipient") as "refund"
    FROM eth_transfers t
    INNER JOIN pairs p
    ON "from" = p."pair" OR "to" = p."pair"
    GROUP BY "tx_hash", "pair"
) foo
)

,transfers AS (
SELECT t."evt_tx_hash", t."from", t."contract_address", array_agg(t."tokenId"::int ORDER BY t."tokenId" ASC) as "transferIds"
FROM erc721."ERC721_evt_Transfer" t
WHERE t."evt_tx_hash" in (select DISTINCT "tx_hash" from swaps)
GROUP BY t."evt_tx_hash", t."contract_address", t."from"
)

,swap_details AS (
SELECT
    s."call_block_time" as "block_time",
    t."contract_address" as "nft_contract_address",
    t."transferIds" as "nft_token_ids_array",
    cardinality(t."transferIds") as "number_of_items",
    s."from" as "buyer",
    p."asset_recipient" as "seller",
    p."pair" as "pair_address",
    e."amount" as "eth_amount_raw",
    e."fees" as "eth_fees_raw",
    s."tx_hash"
FROM
transfers t
INNER JOIN eth_pool_transfers e
ON t."evt_tx_hash" = e."tx_hash" AND t."from" = e."pair"
LEFT JOIN swaps s
ON e."tx_hash" = s."tx_hash"
LEFT JOIN pairs p
ON e."pair" = p."pair"
ORDER BY s."call_block_time"
)


select * FROM swap_details