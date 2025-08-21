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

def check_project_balance_revised():
    """
    予算事業IDごとに、繰越金などを含めた実質的な予算総額と支出総額を比較し、
    矛盾が疑われる事業をリストアップする。
    """

    # ... (設定ファイル読み込み部分は変更なし)
    settings = load_settings()
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    results_folder = PROJECT_ROOT / settings['query_runner']['results_folder']
    
    if not db_file_path.is_file():
        print(f"[エラー] DBファイル '{db_file_path}' が見つかりません。")
        sys.exit(1)

    print(f"--- データベース '{db_file_path}' の事業ごと予算・支出バランスをチェックします (改訂版) ---")
    con = duckdb.connect(database=str(db_file_path), read_only=True)

    # ▼▼▼【変更点】BudgetTotalの計算に「前年度からの繰越し」などを追加▼▼▼
    query = """
    WITH
    BudgetTotal AS (
        -- 事業ごとに、実質的に使えるお金の総額を計算
        SELECT
            予算事業ID,
            事業名,
            SUM("当初予算（合計）") AS "当初予算合計",
            SUM("補正予算（合計）") AS "補正予算合計",
            SUM("前年度からの繰越し（合計）") AS "繰越金合計",
            SUM("予備費等（合計）") AS "予備費等合計",
            SUM("計（歳出予算現額合計）") AS "歳出予算現額合計"
        FROM "予算・執行_サマリ"
        GROUP BY 予算事業ID, 事業名
    ),
    ExpenditureTotal AS (
        -- 事業ごとに、全明細行の支出額を合計
        SELECT
            予算事業ID,
            SUM("金額") AS 事業全体の支出総額
        FROM "支出先_支出情報"
        WHERE "金額" IS NOT NULL -- マイナスも含む全ての支出を合計
        GROUP BY 予算事業ID
    )
    -- 予算と支出の集計結果を結合し、支出が予算を超えている事業のみを抽出
    SELECT
        b.予算事業ID,
        b.事業名,
        b."歳出予算現額合計" AS "実質的な予算総額",
        e.事業全体の支出総額,
        (e.事業全体の支出総額 - b."歳出予算現額合計") AS "超過額",
        -- 参考情報として内訳も表示
        b."当初予算合計",
        b."補正予算合計",
        b."繰越金合計",
        b."予備費等合計"
    FROM
        BudgetTotal AS b
    JOIN
        ExpenditureTotal AS e ON b.予算事業ID = e.予算事業ID
    WHERE
        -- 支出総額が、歳出予算現額（繰越等を含む合計）を超えているものを抽出
        e.事業全体の支出総額 > b."歳出予算現額合計"
    ORDER BY
        超過額 DESC;
    """
    
    print("  - チェック中: 支出総額が実質的な予算総額を超過している事業...")
    # ... (これ以降のロジックは変更なし)
    try:
        df = con.execute(query).fetchdf()
        if not df.empty:
            print(f"    -> {len(df)}件の矛盾が疑われる事業を検出しました。")
        else:
            print("    -> 矛盾は見つかりませんでした。")
    except Exception as e:
        print(f"    [エラー] クエリの実行に失敗しました: {e}")
        con.close()
        return

    con.close()

    if df.empty:
        print("\n[成功] すべての事業で、支出総額が実質的な予算総額の範囲内に収まっていました。")
        return

    print(f"\n--- 予算超過が疑われる事業リスト ({len(df)}件) ---")
    print(df)
    print("---------------------------------------------")

    output_filename = "project_balance_issue_list_revised.csv"
    output_path = results_folder / output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[成功] 詳細なリストを '{output_path}' に保存しました。")


if __name__ == '__main__':
    check_project_balance_revised()