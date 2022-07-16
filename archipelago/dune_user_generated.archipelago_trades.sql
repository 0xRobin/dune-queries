CREATE OR REPLACE view dune_user_generated.archipelago_trades AS(
WITH trades AS (SELECT
a."evt_block_time" as "block_time",
f."tokenAddress" as "nft_contract_address",
f."tokenId",
"cost"/10^coalesce(t."decimals",18) as price,
coalesce(t."symbol",a."currency"::text) as currency,
"buyer",
"seller",
a."evt_tx_hash" as "tx_hash",
(CASE
    WHEN f."tokenAddress" in ('\xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270', '\x059EDD72Cd353dF5106D2B9cC5ab83a52287aC3a') THEN 'Art Blocks'
    WHEN f."tokenAddress" = '\x0A1BBD57033F57E7B6743621b79fCB9Eb2CE3676' THEN 'Bright Moments'
    ELSE '' END) as "platform"
FROM archipelago."ArchipelagoMarket_evt_Trade" a
LEFT JOIN erc20.tokens t
ON a.currency = t.contract_address
LEFT JOIN archipelago."ArchipelagoMarket_evt_TokenTrade" f
ON a."evt_tx_hash" = f."evt_tx_hash" AND a."tradeId" = f."tradeId"
)

, art_blocks_info AS (
SELECT "project_id", "project_name", coalesce(artist."_projectArtistName",'') as artist_name
FROM (
SELECt * FROM (
    SELECT * FROM (
        SELECT  ROW_NUMBER() OVER (
        		ORDER BY call_block_number ASC, "call_trace_address" ASC
        	) + 2 as "project_id", t."_projectName" as "project_name"
            FROM artblocks."GenArt721Core_call_addProject" t
            WHERE call_success
        ) foo
        WHERE "project_id" NOT IN (SELECT "_projectId" from artblocks."GenArt721Core_call_updateProjectName" WHERE call_success)
    ) foo
    UNION
    SELECT t."_projectId" as "project_id", t."_projectName" as "project_name"
    FROM artblocks."GenArt721Core_call_updateProjectName" t
    INNER JOIN (SELECT "_projectId", max(call_block_number) as block
        FROM artblocks."GenArt721Core_call_updateProjectName"
        WHERE call_success
        GROUP BY "_projectId") t2
    ON t.call_block_number = t2.block
    WHERE t.call_success
) foo
LEFT JOIN (
SELECT t."_projectId", t."_projectArtistName"
    FROM artblocks."GenArt721Core_call_updateProjectArtistName" t
    INNER JOIN (SELECT "_projectId", max(call_block_number) as block
        FROM artblocks."GenArt721Core_call_updateProjectArtistName"
        WHERE call_success
        GROUP BY "_projectId") t2
    ON t.call_block_number = t2.block AND t."_projectId" = t2."_projectId"
    WHERE t.call_success
) artist
ON artist."_projectId" = "project_id"
ORDER BY "project_id"
)


, bricht_moments_info AS (
SELECT "project_id", "project_name", coalesce(artist."_projectArtistName",'') as artist_name
FROM (
SELECt * FROM (
    SELECT * FROM (
        SELECT  ROW_NUMBER() OVER (
        		ORDER BY call_block_number ASC, "call_trace_address" ASC
        	) - 1 as "project_id", t."_projectName" as "project_name"
            FROM brightmoments."GenArt721CoreV2_BrightMoments_call_addProject" t
            WHERE call_success
        ) foo
        WHERE "project_id" NOT IN (SELECT "_projectId" from brightmoments."GenArt721CoreV2_BrightMoments_call_updateProjectName" WHERE call_success)
    ) foo
    UNION
    SELECT t."_projectId" as "project_id", t."_projectName" as "project_name"
    FROM brightmoments."GenArt721CoreV2_BrightMoments_call_updateProjectName" t
    INNER JOIN (SELECT "_projectId", max(call_block_number) as block
        FROM brightmoments."GenArt721CoreV2_BrightMoments_call_updateProjectName"
        WHERE call_success
        GROUP BY "_projectId") t2
    ON t.call_block_number = t2.block
    WHERE t.call_success
) foo
LEFT JOIN (SELECT t."_projectId", t."_projectArtistName"
    FROM brightmoments."GenArt721CoreV2_BrightMoments_call_updateProjectArtistName" t
    INNER JOIN (SELECT "_projectId", max(call_block_number) as block
        FROM brightmoments."GenArt721CoreV2_BrightMoments_call_updateProjectArtistName"
        GROUP BY "_projectId") t2
    ON t.call_block_number = t2.block
    WHERE t.call_success
) artist
ON artist."_projectId" = "project_id"
ORDER BY "project_id"
)

,enriched_trades AS (
SELECt t.*,
coalesce(ab."project_name",bm."project_name",erc."name") as "project_name",
coalesce(ab."artist_name",bm."artist_name",'') as "artist_name",
(CASE WHEN "platform" = 'Art Blocks' THEN (t."tokenId"::int % (10^6)::int)
WHEN "platform" = 'Bright Moments' THEN (t."tokenId"::int % (10^6)::int)
ELSE t."tokenId" END) as "token_id",
(CASE WHEN eth."from" = "buyer" THEN 'Listing'
WHEN eth."from" = "seller" Then 'Bid'
ELSE '' END) as "sale_type"
FROM trades t
LEFT JOIN art_blocks_info ab
ON t."platform" = 'Art Blocks' AND ab."project_id" = (t."tokenId"::int / (10^6)::int)
LEFT JOIN bricht_moments_info bm
ON t."platform" = 'Bright Moments' AND bm."project_id" = (t."tokenId"::int / (10^6)::int)
LEFT JOIN nft.tokens erc
ON t."nft_contract_address" = erc."contract_address"
LEFT JOIN ethereum.transactions eth
ON t."tx_hash" = eth."hash"
)

SELECT * FROM enriched_trades ORDER BY block_time
)