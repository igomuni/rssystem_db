-- データの粒度の違いによる重複を防ぐため、先に各テーブルで集計してから結合する
WITH
BudgetSummary AS (
    -- ステップ1: 事業ごとに、会計区分をまたいだ総予算額を計算する
    SELECT
        予算事業ID,
        事業名,
        SUM("計（歳出予算現額合計）") AS 事業全体の総予算額
    FROM
        "予算・執行_サマリ"
    WHERE
        "計（歳出予算現額合計）" IS NOT NULL
    GROUP BY
        予算事業ID, 事業名
),
ExpenditureSummary AS (
    -- ステップ2: 事業ごと、支出先ごとに、支出額を合計する
    -- (同じ支出先が複数行に分かれている場合に対応)
    SELECT
        予算事業ID,
        支出先名,
        SUM("金額") AS 支出先ごとの合計支出額
    FROM
        "支出先_支出情報"
    WHERE
        "金額" IS NOT NULL
    GROUP BY
        予算事業ID, 支出先名
)
-- ステップ3: 集計済みの2つのテーブルを結合する
SELECT
    b.事業名,
    b.事業全体の総予算額,
    e.支出先名,
    e.支出先ごとの合計支出額,
    -- 支出額が総予算額の何パーセントを占めるかを計算
    CAST(e.支出先ごとの合計支出額 AS DOUBLE) / b.事業全体の総予算額 * 100 AS 予算比率
FROM
    BudgetSummary AS b
JOIN
    ExpenditureSummary AS e ON b.予算事業ID = e.予算事業ID
WHERE
    -- ここで気になる事業の予算事業IDを指定
    b.予算事業ID = 7259
ORDER BY
    e.支出先ごとの合計支出額 DESC;