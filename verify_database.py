import duckdb
import pandas as pd
import sys
import json
from pathlib import Path

SETTINGS_FILE = 'project_settings.json'

def load_settings():
    """設定ファイルを読み込む"""
    try:
        with Path(SETTINGS_FILE).open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' の読み込みに失敗しました: {e}", file=sys.stderr)
        sys.exit(1)

def verify_single_db():
    settings = load_settings()
    try:
        db_file_path_str = settings['database']['output_db_file']
    except KeyError as e:
        print(f"[エラー] 設定ファイルに必要なキー {e} がありません。", file=sys.stderr)
        sys.exit(1)

    print(f"--- データベース '{db_file_path_str}' の検証を開始します ---")

    db_path = Path(db_file_path_str)
    if not db_path.is_file():
        print(f"\n[エラー] 検証対象のデータベースファイル '{db_path}' が見つかりません。")
        sys.exit(1)
        
    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
    except Exception as e:
        print(f"\n[エラー] データベースへの接続に失敗しました: {e}", file=sys.stderr)
        return

    # --- ステップ1: インデックス用テーブルの確認 ---
    print("\n[ステップ1] インデックス用テーブル 'table_index' の内容を確認します...")
    try:
        index_df = con.execute("SELECT * FROM table_index ORDER BY view_name").fetchdf()
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', 1000)
        print(f" -> 'table_index' を検出しました。{len(index_df)}件のマッピング情報が見つかりました。")
        print(index_df)
    except Exception as e:
        print(f"\n[エラー] 'table_index' の読み込みに失敗しました: {e}", file=sys.stderr)
        con.close()
        return

    # --- ステップ2: 各テーブルとVIEWの存在と行数を確認 ---
    print("\n[ステップ2] 各テーブル/VIEWの存在と行数を確認します...")
    all_ok = True
    for _, row in index_df.iterrows():
        table_name = row['table_name']
        view_name = row['view_name']
        try:
            # テーブルの行数をカウント
            row_count_result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = row_count_result[0]
            # VIEWの存在を確認 (軽量なクエリ)
            con.execute(f'SELECT 1 FROM "{view_name}" LIMIT 1')
            print(f"  - Table: {table_name}, View: {view_name}, Rows: {row_count:,} [OK]")
        except Exception as e:
            print(f"  - Table: {table_name}, View: {view_name} -> 検証中にエラー: {e} [エラー！]")
            all_ok = False
    
    if not all_ok:
        print("\n[警告] いくつかのテーブル/VIEWで問題が検出されました。")

    # --- ステップ3: データ内容のサンプル表示 (VIEWを使用) ---
    print("\n[ステップ3] データ内容のサンプルをVIEW経由で表示します（先頭3件）...")
    sample_views = index_df['view_name'].head(3).tolist()
    for view_name in sample_views:
        try:
            print(f'\n--- サンプル: VIEW "{view_name}" の先頭5行 ---')
            sample_df = con.execute(f'SELECT * FROM "{view_name}" LIMIT 5').fetchdf()
            pd.set_option('display.max_columns', 10)
            print(sample_df)
        except Exception as e:
            print(f" -> VIEW '{view_name}' のサンプル取得中にエラー: {e}")
            
    con.close()
    print("\n--- 検証が完了しました ---")

if __name__ == '__main__':
    verify_single_db()