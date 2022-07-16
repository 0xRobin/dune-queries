SELECT
"block_time",
"project_name",
"artist_name",
"token_id",
"price",
"currency",
"sale_type",
coalesce((labels.get("buyer",'ens name reverse'))[1],CONCAT('0',substring("buyer"::text from 2 for 4),'..',substring("buyer"::text from 40))) as "buyer",
coalesce((labels.get("seller",'ens name reverse'))[1],CONCAT('0',substring("seller"::text from 2 for 4),'..',substring("seller"::text from 40))) as "seller",
"platform",
CONCAT('<a href="https://archipelago.art/collections/',
lower(replace(replace("project_name",' - ','-'),' ','-'))::text,'/',
("token_id")::text,
'" target="_blank" >[ 🎨 ]</a>') as "[archipelago]",
CONCAT('<a href="https://etherscan.io/tx/0', substring("tx_hash"::text from 2),'" target="_blank" >'
,'[ 🔗 ]','</a>') as "[etherscan]"
FROM dune_user_generated.archipelago_trades
ORDER BY "block_time" DESC