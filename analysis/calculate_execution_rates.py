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

def calculate_rates():
    """全事業の予算執行率を計算し、CSVに出力する"""
    settings = load_settings()
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    results_folder = PROJECT_ROOT / settings['query_runner']['results_folder']
    
    if not db_file_path.is_file():
        print(f"[エラー] DBファイル '{db_file_path}' が見つかりません。")
        sys.exit(1)

    print(f"--- データベース '{db_file_path}' から執行率を計算します ---")
    con = duckdb.connect(database=str(db_file_path), read_only=True)

    # 複数の会計区分を考慮し、事業IDごとに予算と執行額を合計するSQL
    query = """
    WITH BudgetSummary AS (
        SELECT
            予算事業ID,
            事業名,
            SUM("計（歳出予算現額合計）") AS 総予算額,
            SUM("執行額（合計）") AS 総執行額
        FROM "予算・執行_サマリ"
        GROUP BY 予算事業ID, 事業名
    )
    SELECT
        b.予算事業ID,
        b.事業名,
        o.府省庁,
        b.総予算額,
        b.総執行額,
        -- 0除算を避けるためのCASE文
        CASE
            WHEN b.総予算額 = 0 THEN NULL
            ELSE (CAST(b.総執行額 AS DOUBLE) / b.総予算額) * 100
        END AS 執行率
    FROM BudgetSummary AS b
    LEFT JOIN "基本情報_組織情報" AS o ON b.予算事業ID = o.予算事業ID;
    """
    
    try:
        df = con.execute(query).fetchdf()
        print(f" -> {len(df)}件の事業について執行率を計算しました。")
    except Exception as e:
        print(f"[エラー] クエリの実行に失敗しました: {e}")
        con.close()
        return

    con.close()

    # 結果をCSVに出力
    output_filename = "execution_rates_summary.csv"
    output_path = results_folder / output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[成功] 分析用のサマリーファイルを '{output_path}' に保存しました。")


if __name__ == '__main__':
    calculate_rates()