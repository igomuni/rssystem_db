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

def validate_split_revised():
    """
    支出情報テーブルの「サマリー」と「明細」の金額の整合性を、
    重複を排除した形で検証する（改訂版）。
    """
    settings = load_settings()
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    results_folder = PROJECT_ROOT / settings['query_runner']['results_folder']
    
    if not db_file_path.is_file():
        print(f"[エラー] DBファイル '{db_file_path}' が見つかりません。")
        sys.exit(1)

    print(f"--- データベース '{db_file_path}' を使って、サマリーと明細の整合性を検証します (改訂版) ---")
    con = duckdb.connect(database=str(db_file_path), read_only=True)

    # ▼▼▼【変更点】重複を排除するため、両側で先に集計するロジックに変更▼▼▼
    query = """
    WITH
    SummaryGrouped AS (
        -- サマリー側を、事業IDとブロック番号でグループ化して合計
        -- これにより、会計区分の違いによる重複行を1つにまとめる
        SELECT
            予算事業ID,
            支出先ブロック番号,
            SUM("ブロックの合計支出額") AS summary_total
        FROM "支出先_支出情報_サマリー"
        WHERE "ブロックの合計支出額" IS NOT NULL
        GROUP BY 予算事業ID, 支出先ブロック番号
    ),
    DetailsGrouped AS (
        -- 明細側も同様にグループ化して合計
        SELECT
            予算事業ID,
            支出先ブロック番号,
            SUM("金額") AS details_sum
        FROM "支出先_支出情報_明細"
        GROUP BY 予算事業ID, 支出先ブロック番号
    )
    -- 集計済みのテーブル同士を結合
    SELECT
        s.予算事業ID,
        org.事業名,
        s.支出先ブロック番号,
        s.summary_total AS "サマリー側の合計額",
        d.details_sum AS "明細側の合計額",
        (s.summary_total - d.details_sum) AS "差額"
    FROM
        SummaryGrouped s
    JOIN
        DetailsGrouped d ON s.予算事業ID = d.予算事業ID AND s.支出先ブロック番号 = d.支出先ブロック番号
    LEFT JOIN
        "基本情報_組織情報" org ON s.予算事業ID = org.予算事業ID
    WHERE
        ABS(s.summary_total - d.details_sum) > 1 -- 差額が1円より大きいもの
    ORDER BY
        ABS("差額") DESC;
    """
    
    print("  - 分析中: ブロックごとにサマリーと明細の合計額を比較しています...")
    try:
        df = con.execute(query).fetchdf()
        if df.empty:
            print("    -> 素晴らしい！すべてのブロックで金額が一致しました。")
        else:
            print(f"    -> {len(df)}件のブロックで金額の不一致が検出されました。")
    except Exception as e:
        print(f"    [エラー] クエリの実行に失敗しました: {e}")
        con.close()
        return

    con.close()

    if df.empty:
        print("\n[結論] 仮説は証明されました。サマリーと明細の分割は正しく機能しています。")
        return

    print(f"\n--- 金額が一致しなかったブロックのリスト ({len(df)}件) ---")
    print(df)
    print("--------------------------------------------------")

    output_filename = "summary_details_discrepancy_list_revised.csv"
    output_path = results_folder / output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[成功] 詳細な不一致リストを '{output_path}' に保存しました。")


if __name__ == '__main__':
    validate_split_revised()