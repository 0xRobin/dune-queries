SELECT
CONCAT('<a href="https://archipelago.art/collections/',
lower(replace(replace("project_name",' - ','-'),' ','-'))::text,'/',
("token_id")::text,
'" target="_blank" >🎨</a>') as "link",
concat("project_name",' #',"token_id") as "Artwork",
"price",
"currency",
"artist_name",
coalesce((labels.get("buyer",'ens name reverse'))[1],CONCAT('0',substring("buyer"::text from 2 for 4),'..',substring("buyer"::text from 40))) as "buyer",
CONCAT('<a href="https://etherscan.io/tx/0', substring("tx_hash"::text from 2),'" target="_blank" >'
,'[ 🔗 ]','</a>') as "[etherscan]",
"block_time"
FROM dune_user_generated.archipelago_trades
ORDER BY "price" DESC
LIMIT 12