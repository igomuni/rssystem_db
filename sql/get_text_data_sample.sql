-- RAGシステムのベクトル検索の精度をデバッグするために、
-- 埋め込みに使われたテキストデータのランダムなサンプルを100件抽出する
SELECT
    o.事業名,
    d.支出先名,
    d.契約概要
FROM
    "支出先_支出情報_明細" AS d
LEFT JOIN
    "基本情報_組織情報" AS o ON d.予算事業ID = o.予算事業ID
WHERE
    d.契約概要 IS NOT NULL
    AND o.事業名 IS NOT NULL
    AND d.支出先名 IS NOT NULL
ORDER BY
    -- 全体からランダムにサンプリングする
    random()
LIMIT 100;