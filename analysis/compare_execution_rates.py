import duckdb
import pandas as pd
import sys
import json
from pathlib import Path

# --- (パス解決とload_settings関数は変更なし) ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path().cwd()
SETTINGS_FILE = PROJECT_ROOT / 'project_settings.json'

def load_settings():
    # ... (省略)
    try:
        with SETTINGS_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' の読み込みに失敗: {e}", file=sys.stderr)
        sys.exit(1)

def compare_rates():
    """
    元データにある執行率と、支出総額から計算した実態ベースの執行率を比較する。
    """
    settings = load_settings()
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    results_folder = PROJECT_ROOT / settings['query_runner']['results_folder']
    
    if not db_file_path.is_file():
        print(f"[エラー] DBファイル '{db_file_path}' が見つかりません。")
        sys.exit(1)

    print(f"--- データベース '{db_file_path}' を使って執行率を比較します ---")
    con = duckdb.connect(database=str(db_file_path), read_only=True)

    # 2種類の執行率を計算・比較するSQLクエリ
    query = """
    WITH
    BudgetSummary AS (
        -- 事業ごとに、予算現額の合計と、執行率の加重平均を計算
        SELECT
            予算事業ID,
            事業名,
            SUM("計（歳出予算現額合計）") AS 事業全体の予算総額,
            -- 執行率がNULLでないレコードのみで加重平均を計算
            SUM("執行率" * "計（歳出予算現額合計）") / SUM("計（歳出予算現額合計）") AS 元データの執行率
        FROM "予算・執行_サマリ"
        WHERE "計（歳出予算現額合計）" IS NOT NULL AND "計（歳出予算現額合計）" != 0 AND "執行率" IS NOT NULL
        GROUP BY 予算事業ID, 事業名
    ),
    ExpenditureSummary AS (
        -- 事業ごとに、支出総額を計算
        SELECT
            予算事業ID,
            SUM("金額") AS 事業全体の支出総額
        FROM "支出先_支出情報"
        WHERE "金額" IS NOT NULL
        GROUP BY 予算事業ID
    )
    -- 2つの集計結果を結合
    SELECT
        b.予算事業ID,
        b.事業名,
        b.元データの執行率,
        -- 実態ベースの執行率を計算 (0除算を避ける)
        CASE
            WHEN b.事業全体の予算総額 = 0 THEN NULL
            ELSE (e.事業全体の支出総額 / b.事業全体の予算総額) * 100
        END AS 実態ベースの計算上の執行率,
        b.事業全体の予算総額,
        e.事業全体の支出総額
    FROM
        BudgetSummary b
    JOIN
        ExpenditureSummary e ON b.予算事業ID = e.予算事業ID
    -- 2つの執行率の差が大きい順に並べる
    ORDER BY
        ABS(元データの執行率 - 実態ベースの計算上の執行率) DESC
    LIMIT 100; -- 差が大きいトップ100件を表示
    """
    
    print("  - 分析中: 2種類の執行率を計算し、比較しています...")
    try:
        df = con.execute(query).fetchdf()
        if df.empty:
            print("    -> 比較可能なデータが見つかりませんでした。")
            con.close()
            return
        print(f"    -> {len(df)}件の事業について比較結果を生成しました。")
    except Exception as e:
        print(f"    [エラー] クエリの実行に失敗しました: {e}")
        con.close()
        return

    con.close()

    print("\n--- 執行率の乖離が大きい事業 トップ10 ---")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 120)
    print(df.head(10).to_string())
    print("-----------------------------------------")

    # 結果をCSVファイルに保存
    output_filename = "execution_rate_comparison.csv"
    output_path = results_folder / output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[成功] 詳細な比較レポートを '{output_path}' に保存しました。")


if __name__ == '__main__':
    compare_rates()