import duckdb
import pandas as pd
import sys
from pathlib import Path

# --- 設定 ---
DB_FILES_FOLDER = 'duckdb_files'

def verify_individual_dbs():
    """
    指定されたフォルダ内のすべての.duckdbファイルを検証し、
    レポートとデータサンプルを出力します。
    """
    print(f"--- フォルダ '{DB_FILES_FOLDER}' の検証を開始します ---")

    db_folder_path = Path(DB_FILES_FOLDER)
    if not db_folder_path.is_dir():
        print(f"\n[エラー] 検証対象のフォルダ '{DB_FILES_FOLDER}' が見つかりません。")
        sys.exit(1)

    db_files = sorted(list(db_folder_path.glob('*.duckdb')))
    if not db_files:
        print(f"\n[エラー] フォルダ '{DB_FILES_FOLDER}' 内に.duckdbファイルが見つかりません。")
        sys.exit(1)

    print(f"\n[ステップ1] {len(db_files)}個のデータベースファイルの構造と内容を検証します...")
    
    all_files_ok = True
    verified_tables = []

    for db_path in db_files:
        try:
            con = duckdb.connect(database=str(db_path), read_only=True)
            
            # メタデータを確認
            meta_df = con.execute("SELECT original_filename FROM _metadata").fetchdf()
            original_filename = meta_df['original_filename'][0] if not meta_df.empty else 'N/A'

            # データテーブルを確認
            tables_df = con.execute("SELECT table_name FROM duckdb_tables() WHERE table_name != '_metadata'").fetchdf()
            if tables_df.empty:
                raise ValueError("データテーブルが見つかりません。")
            
            table_name = tables_df['table_name'][0]
            row_count_result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = row_count_result[0]

            print(f"  - {db_path.name}: {row_count:,} 行 (元ファイル: {original_filename}) [OK]")
            verified_tables.append({'path': db_path, 'table_name': table_name})
            
            con.close()
        except Exception as e:
            print(f"  - {db_path.name}: 検証中にエラーが発生しました -> {e} [エラー！]")
            all_files_ok = False

    if not all_files_ok:
        print("\n[警告] いくつかのファイルで問題が検出されました。")

    # --- データ内容のサンプル表示 ---
    print("\n[ステップ2] データ内容のサンプルを表示します（先頭3ファイル、各5行）...")
    sample_files = verified_tables[:3]

    if not sample_files:
        print(" -> サンプル表示する正常なファイルがありませんでした。")
    else:
        for item in sample_files:
            try:
                con = duckdb.connect(database=str(item['path']), read_only=True)
                table_name = item['table_name']
                print(f"\n--- サンプル: ファイル '{item['path'].name}' / テーブル '{table_name}' の先頭5行 ---")
                sample_df = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
                pd.set_option('display.width', 200) # 表示幅を調整
                pd.set_option('display.max_columns', 10) # 表示列数を制限
                print(sample_df)
                print("-" * (len(str(item['path'].name)) + 30))
                con.close()
            except Exception as e:
                print(f" -> ファイル '{item['path'].name}' のサンプル取得中にエラー: {e}")

    print("\n--- 検証が完了しました ---")

if __name__ == '__main__':
    verify_individual_dbs()