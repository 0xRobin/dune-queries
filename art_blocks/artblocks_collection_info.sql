WITH art_blocks_info AS (
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

SELECT * FROM art_blocks_info