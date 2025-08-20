-- find_road_projects.sql の結果をサブクエリとして使い、
-- 道路関連事業の総予算額と事業数を計算する
WITH RoadProjects AS (
    -- find_road_projects.sql の内容をここにコピー
    SELECT
        概要.予算事業ID,
        予算."計（歳出予算現額合計）" AS 予算額
    FROM
        "基本情報_事業概要等" AS 概要
    JOIN "予算・執行_サマリ" AS 予算 ON 概要.予算事業ID = 予算.予算事業ID
    WHERE
        (概要.事業名 LIKE '%道路%' OR 概要.事業名 LIKE '%高速道路%' OR 概要.事業名 LIKE '%国道%' OR 概要.事業名 LIKE '%インフラ%' OR 概要.事業の目的 LIKE '%道路%' OR 概要.事業の目的 LIKE '%交通網%')
        AND 予算."計（歳出予算現額合計）" > 0
)
SELECT
    COUNT(予算事業ID) AS 道路関連事業の数,
    SUM(予算額) AS 道路関連事業の総予算額
FROM
    RoadProjects;