import duckdb
import pandas as pd
import sys
import json
from pathlib import Path

# --- (パス解決コードは変更なし) ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path().cwd()

SETTINGS_FILE = PROJECT_ROOT / 'project_settings.json'

CHECKS_TO_PERFORM = [
    {
        'description': "【要注意】明細行なのに支出先名が空欄",
        'view_name': "支出先_支出情報",
        'condition': """ ("金額" IS NOT NULL AND "金額" != 0) AND ("支出先名" IS NULL OR "支出先名" = '') """,
        'columns_to_show': ['予算事業ID', '事業名', '支出先ブロック名', '金額', '契約概要']
    },
    {
        'description': "【要注意】明細行なのに契約情報が両方空欄",
        'view_name': "支出先_支出情報",
        'condition': """ ("金額" IS NOT NULL AND "金額" != 0) AND ("契約方式等" IS NULL OR "契約方式等" = '') AND ("契約概要" IS NULL OR "契約概要" = '') """,
        'columns_to_show': ['予算事業ID', '事業名', '支出先名', '金額']
    },
    {
        'description': "【要注意】事業の目的が空欄",
        'view_name': "基本情報_事業概要等",
        'condition': """ "事業の目的" IS NULL OR "事業の目的" = '' """,
        'columns_to_show': ['予算事業ID', '事業名', '府省庁']
    },
    {
        'description': "【参考】支出金額がマイナス",
        'view_name': "支出先_支出情報",
        'condition': """ "金額" < 0 """,
        'columns_to_show': ['予算事業ID', '事業名', '支出先名', '金額', '契約概要']
    },
    {
        'description': "【参考】支出に関する金額情報が一切ない",
        'view_name': "支出先_支出情報",
        'condition': """ ("金額" IS NULL OR "金額" = 0) AND ("ブロックの合計支出額" IS NULL OR "ブロックの合計支出額" = 0) AND ("支出先の合計支出額" IS NULL OR "支出先の合計支出額" = 0) """,
        'columns_to_show': ['予算事業ID', '事業名', '支出先ブロック名', '支出先名']
    },
    {
        'description': "【参考】事業全体の予算額が0以下",
        'view_name': "予算・執行_サマリ",
        'is_aggregated_query': True,
        'query': """
            SELECT '【参考】事業全体の予算額が0以下' AS "問題の理由", 予算事業ID, 事業名, SUM("計（歳出予算現額合計）") AS "総予算額"
            FROM "予算・執行_サマリ"
            GROUP BY 予算事業ID, 事業名
            HAVING SUM("計（歳出予算現額合計）") <= 0
        """
    },
    {
        # ▼▼▼【変更点】このチェック項目の condition を更新▼▼▼
        'description': "【参考】役割が空欄だが金額のあるサマリー行",
        'view_name': "支出先_支出情報",
        # 「金額」が空で、「役割」も空だが、「ブロック合計額」には値がある、という条件
        'condition': """ ("金額" IS NULL OR "金額" = 0) AND ("事業を行う上での役割" IS NULL OR "事業を行う上での役割" = '') AND ("ブロックの合計支出額" IS NOT NULL AND "ブロックの合計支出額" != 0) """,
        'columns_to_show': ['予算事業ID', '事業名', '支出先ブロック名', 'ブロックの合計支出額']
    }
]

def load_settings():
    # ... (変更なし)
    try:
        with SETTINGS_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' の読み込みに失敗: {e}", file=sys.stderr)
        sys.exit(1)

def generate_report():
    # ... (変更なし)
    settings = load_settings()
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    results_folder = PROJECT_ROOT / settings['query_runner']['results_folder']
    
    if not db_file_path.is_file():
        print(f"[エラー] DBファイル '{db_file_path}' が見つかりません。")
        sys.exit(1)

    con = duckdb.connect(database=str(db_file_path), read_only=True)
    
    all_issues_dfs = []
    print("--- データ品質チェックを開始します ---")

    for check in CHECKS_TO_PERFORM:
        print(f"  - チェック中: {check['description']}...")
        try:
            if check.get('is_aggregated_query'):
                df = con.execute(check['query']).fetchdf()
            else:
                columns = '", "'.join(check['columns_to_show'])
                query = f"""
                    SELECT
                        '{check['description']}' AS "問題の理由",
                        "{columns}"
                    FROM
                        "{check['view_name']}"
                    WHERE
                        {check['condition']}
                """
                df = con.execute(query).fetchdf()
            
            if not df.empty:
                print(f"    -> {len(df)}件の問題を検出しました。")
                all_issues_dfs.append(df)
            else:
                print("    -> 問題は見つかりませんでした。")
        except Exception as e:
            print(f"    [エラー] チェック中にエラーが発生しました: {e}")

    con.close()

    if not all_issues_dfs:
        print("\n[成功] すべてのチェックをパスしました。データ品質に関する問題は見つかりませんでした。")
        return

    final_report_df = pd.concat(all_issues_dfs, ignore_index=True)

    print("\n--- データ品質チェック サマリー ---")
    print(f"合計 {len(final_report_df)}件 の潜在的な問題を検出しました。")
    print("\n問題の内訳:")
    print(final_report_df['問題の理由'].value_counts().to_string())
    print("---------------------------------")

    output_filename = "data_quality_long_list.csv"
    output_path = results_folder / output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    final_report_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[成功] 詳細な問題リストを '{output_path}' に保存しました。")


if __name__ == '__main__':
    generate_report()