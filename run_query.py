import duckdb
import pandas as pd
import argparse
import sys
import json
from pathlib import Path

# --- 設定ファイル名を定数として定義 ---
SETTINGS_FILE = 'project_settings.json'

def load_settings():
    """設定ファイルを読み込み、設定内容の辞書を返す"""
    try:
        settings_path = Path(SETTINGS_FILE)
        with settings_path.open('r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' が見つかりません。", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' のJSON形式が正しくありません。", file=sys.stderr)
        sys.exit(1)

# (run_sql_query 関数は変更なし)
def run_sql_query(query_str: str, source_file: str, db_file_path: str, output_csv_path: Path = None):
    # ... (この関数の中身は変更なし) ...
    db_path = Path(db_file_path)
    if not db_path.is_file():
        print(f"[エラー] データベースファイル '{db_file_path}' が見つかりません。")
        sys.exit(1)

    con = duckdb.connect(database=str(db_path), read_only=True)

    print(f"\n--- 以下のSQLクエリを実行します (from: {source_file}) ---")
    print(query_str)
    print("-----------------------------------------------------" + "-" * len(source_file))

    try:
        result_df = con.execute(query_str).fetchdf()
        
        print(f"\n[成功] クエリが完了し、{len(result_df)}件の結果を取得しました。")
        
        pd.set_option('display.max_rows', 100)
        pd.set_option('display.max_columns', 50)
        pd.set_option('display.width', 200)
        print("\n--- クエリ結果 ---")
        print(result_df)
        print("------------------")

        if output_csv_path:
            output_csv_path.parent.mkdir(parents=True, exist_ok=True)
            result_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
            print(f"\n[成功] 結果を '{output_csv_path}' に保存しました。")

    except Exception as e:
        print(f"\n[エラー] SQLクエリの実行中にエラーが発生しました: {e}", file=sys.stderr)
    finally:
        con.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="DuckDBファイル群に対してSQLクエリを実行します。")
    parser.add_argument('-q', '--query', type=str, help="実行したいSQLクエリが書かれた.sqlファイルのパス。")
    parser.add_argument('-o', '--output', type=str, help="結果を保存するCSVの「ファイル名」。")
    parser.add_argument('--no-output', action='store_true', help="結果をファイルに出力しません。")
    args = parser.parse_args()

    settings = load_settings()
    try:
        db_file = settings['database']['output_db_file']
        results_folder = settings['query_runner']['results_folder']
        default_query_file = settings['query_runner']['default_query_file']
        default_output_filename = settings['query_runner']['default_output_filename']
        # ▼▼▼【変更点1】sqlフォルダのパスも設定から読み込む▼▼▼
        query_dir = Path(settings['query_runner'].get('query_directory', 'sql'))
    except KeyError as e:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' に必要なキー {e} がありません。", file=sys.stderr)
        sys.exit(1)

    # ▼▼▼【変更点2】クエリファイルのパス解決ロジックを強化▼▼▼
    query_file_path_str = args.query or default_query_file
    query_path = Path(query_file_path_str)

    # もし指定されたパスにフォルダが含まれていなければ (例: 'my_query.sql')、
    # かつそれがデフォルトのファイル名でなければ、'sql/'フォルダの中を探しに行く
    if not query_path.parent.name and query_path.name != default_query_file:
        resolved_path = query_dir / query_path
        if resolved_path.exists():
            query_path = resolved_path
    
    try:
        sql_to_run = query_path.read_text(encoding='utf-8')
    except FileNotFoundError:
        print(f"[エラー] 指定されたクエリファイル '{query_path}' が見つかりません。", file=sys.stderr)
        sys.exit(1)
        
    output_full_path = None
    if not args.no_output:
        output_filename = args.output or default_output_filename
        output_full_path = Path(results_folder) / output_filename
        
    run_sql_query(sql_to_run, str(query_path), db_file, output_full_path)