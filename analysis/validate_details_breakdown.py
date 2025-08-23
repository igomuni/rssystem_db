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

def validate_breakdown():
    """
    「支出明細」と「費目・使途」の金額の整合性を検証する。
    """
    settings = load_settings()
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    results_folder = PROJECT_ROOT / settings['query_runner']['results_folder']
    
    if not db_file_path.is_file():
        print(f"[エラー] DBファイル '{db_file_path}' が見つかりません。")
        sys.exit(1)

    print(f"--- データベース '{db_file_path}' を使って、明細と内訳の整合性を検証します ---")
    con = duckdb.connect(database=str(db_file_path), read_only=True)

    # 契約ごとの金額と、その内訳である費目・使途の合計金額を比較するSQLクエリ
    query = """
    WITH
    DetailsAmount AS (
        -- 契約ごとに、明細行に記載された金額を取得
        -- 契約を特定するために、複数のキーを組み合わせる
        SELECT
            予算事業ID, 支出先ブロック番号, 支出先名, 法人番号, 契約概要,
            "金額" AS contract_amount
        FROM "支出先_支出情報_明細"
        WHERE "金額" IS NOT NULL
    ),
    BreakdownSum AS (
        -- 同じキーの組み合わせで、費目・使途の金額を合計
        SELECT
            予算事業ID, 支出先ブロック番号, 支出先名, 法人番号, 契約概要,
            SUM("金額") AS breakdown_sum
        FROM "支出先_費目・使途"
        WHERE "金額" IS NOT NULL
        GROUP BY 予算事業ID, 支出先ブロック番号, 支出先名, 法人番号, 契約概要
    )
    -- 両者を結合し、金額に差がある契約のみを抽出
    SELECT
        d.予算事業ID,
        org.事業名,
        d.支出先名,
        d.契約概要,
        d.contract_amount AS "契約金額（明細）",
        b.breakdown_sum AS "内訳の合計金額",
        (d.contract_amount - b.breakdown_sum) AS "差額"
    FROM
        DetailsAmount d
    JOIN
        BreakdownSum b 
        ON  d.予算事業ID = b.予算事業ID 
        AND d.支出先ブロック番号 = b.支出先ブロック番号
        AND d.支出先名 = b.支出先名
        -- 法人番号と契約概要はNULLの場合があるので、安全な比較を行う
        AND (d.法人番号 = b.法人番号 OR (d.法人番号 IS NULL AND b.法人番号 IS NULL))
        AND (d.契約概要 = b.契約概要 OR (d.契約概要 IS NULL AND b.契約概要 IS NULL))
    LEFT JOIN
        "基本情報_組織情報" org ON d.予算事業ID = org.予算事業ID
    WHERE
        -- 差額が一定以上あるものだけを抽出（丸め誤差を許容するため）
        ABS(d.contract_amount - b.breakdown_sum) > 1 -- 差額が1円より大きいもの
    ORDER BY
        ABS("差額") DESC;
    """
    
    print("  - 分析中: 契約ごとに明細と内訳の合計額を比較しています...")
    try:
        df = con.execute(query).fetchdf()
        if df.empty:
            print("    -> 素晴らしい！すべての契約で金額が一致しました。")
        else:
            print(f"    -> {len(df)}件の契約で金額の不一致が検出されました。")
    except Exception as e:
        print(f"    [エラー] クエリの実行に失敗しました: {e}")
        con.close()
        return

    con.close()

    if df.empty:
        print("\n[結論] データの整合性は非常に高いです。明細とその内訳は正しく対応しています。")
        return

    print(f"\n--- 金額が一致しなかった契約のリスト ({len(df)}件) ---")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 150)
    print(df)
    print("--------------------------------------------------")

    output_filename = "details_breakdown_discrepancy_list.csv"
    output_path = results_folder / output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[成功] 詳細な不一致リストを '{output_path}' に保存しました。")


if __name__ == '__main__':
    validate_breakdown()