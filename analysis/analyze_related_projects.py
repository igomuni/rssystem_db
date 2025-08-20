import duckdb
import pandas as pd
import argparse
import sys
import json
from pathlib import Path

# --- プロジェクトルートを基準にパスを解決 ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path().cwd()

SETTINGS_FILE = PROJECT_ROOT / 'project_settings.json'
# ---------------------------------------------

def load_settings():
    """設定ファイルを読み込む"""
    try:
        with SETTINGS_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' の読み込みに失敗: {e}", file=sys.stderr)
        sys.exit(1)

def get_top_budget_project_id(con: duckdb.DuckDBPyConnection):
    """予算額が最大の事業のIDと事業名を取得する"""
    print("--- 予算額最大の事業を検索中... ---")
    query = """
        SELECT 予算事業ID, 事業名
        FROM "予算・執行_サマリ"
        WHERE "計（歳出予算現額合計）" IS NOT NULL
        GROUP BY 予算事業ID, 事業名
        ORDER BY SUM("計（歳出予算現額合計）") DESC
        LIMIT 1;
    """
    result = con.execute(query).fetchone()
    if result:
        print(f" -> 見つかりました: ID={result[0]}, 事業名='{result[1]}'")
        return result[0], result[1]
    else:
        print(" -> 見つかりませんでした。")
        return None, None

def analyze_related_projects(business_id: int, business_name: str, con: duckdb.DuckDBPyConnection):
    """指定された事業IDの関連事業を分析し、DataFrameを返す"""
    print(f"\n--- ID: {business_id} ('{business_name}') の関連事業を分析中... ---")
    query = f'SELECT * FROM "基本情報_関連事業" WHERE 予算事業ID = {business_id}'
    
    related_df = con.execute(query).fetchdf()

    if related_df.empty:
        print(" -> この事業には関連事業の登録がありませんでした。")
        return None

    # --- 分析サマリーの表示 ---
    num_related = len(related_df)
    relation_counts = related_df['関連性'].value_counts()

    print("\n--- 分析サマリー ---")
    print(f"関連事業の数: {num_related}件")
    print("\n「関連性」の内訳:")
    print(relation_counts.to_string())
    print("--------------------")

    return related_df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="指定された(または予算最大の)事業の関連事業を分析します。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        'business_id', 
        type=int, 
        nargs='?', # '?'は引数が0個か1個であることを示す
        default=None,
        help="分析したい予算事業ID (例: 7259)。\n指定しない場合は、予算額が最大の事業を自動で分析します。"
    )
    args = parser.parse_args()

    settings = load_settings()
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    results_folder = PROJECT_ROOT / settings['query_runner']['results_folder']
    
    if not db_file_path.is_file():
        print(f"[エラー] DBファイル '{db_file_path}' が見つかりません。`import_zips_to_duckdb.py` を実行してください。")
        sys.exit(1)

    con = duckdb.connect(database=str(db_file_path), read_only=True)

    target_id = args.business_id
    target_name = "指定された事業"

    if target_id is None:
        target_id, target_name = get_top_budget_project_id(con)
        if target_id is None:
            con.close()
            sys.exit(1)
    
    analysis_result_df = analyze_related_projects(target_id, target_name, con)

    if analysis_result_df is not None:
        # 結果をCSVファイルに保存
        output_filename = f"related_projects_for_ID_{target_id}.csv"
        output_path = results_folder / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        analysis_result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n[成功] 詳細な関連事業リストを '{output_path}' に保存しました。")
    
    con.close()