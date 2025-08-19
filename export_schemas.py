import duckdb
import json
import yaml
import sys
import pandas as pd
from pathlib import Path

# --- 設定 ---
DB_FILES_FOLDER = 'duckdb_files'
OUTPUT_FILENAME_BASE = 'schema'

def export_all_formats_from_folder():
    """
    指定されたフォルダ内のすべての.duckdbファイルをスキャンし、
    スキーマ情報を集約してJSONとYAMLの両形式でファイルに出力します。(pathlib使用版)
    """
    print(f"--- フォルダ '{DB_FILES_FOLDER}' 内のDBファイルのスキーマ情報をエクスポートします ---")

    # ▼▼▼【変更点】pathlibを使ってDBファイルを検索▼▼▼
    db_files = list(Path(DB_FILES_FOLDER).glob('*.duckdb'))
    
    if not db_files:
        print(f"\n[エラー] フォルダ '{DB_FILES_FOLDER}' 内に.duckdbファイルが見つかりません。")
        sys.exit(1)

    schema_export_data = {}
    print(f"\n{len(db_files)}個のDBファイルを処理します...")

    for db_path in sorted(db_files):
        try:
            con = duckdb.connect(database=str(db_path), read_only=True)
            
            meta_df = con.execute("SELECT original_filename FROM _metadata").fetchdf()
            original_filename = meta_df['original_filename'][0] if not meta_df.empty else 'N/A'

            tables_df = con.execute("SELECT table_name FROM duckdb_tables() WHERE table_name != '_metadata'").fetchdf()
            if tables_df.empty:
                print(f"[警告] {db_path.name} にデータテーブルが見つかりません。スキップします。")
                con.close()
                continue
            
            table_name = tables_df['table_name'][0]
            
            schema_df = con.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
            columns_info = schema_df[['name', 'type', 'notnull', 'pk']].to_dict('records')
            
            schema_export_data[table_name] = {
                'original_filename': original_filename,
                'database_file': db_path.name,
                'columns': columns_info
            }
            con.close()

        except Exception as e:
            print(f"\n[エラー] ファイル '{db_path.name}' の処理中に問題が発生しました: {e}")

    if not schema_export_data:
        print("\nエクスポートするスキーマ情報がありませんでした。処理を終了します。")
        return

    json_path = Path(f"{OUTPUT_FILENAME_BASE}.json")
    with json_path.open('w', encoding='utf-8') as f:
        json.dump(schema_export_data, f, indent=4, ensure_ascii=False)
    print(f"\n[成功] スキーマ情報を '{json_path}' にエクスポートしました。")

    yaml_path = Path(f"{OUTPUT_FILENAME_BASE}.yaml")
    with yaml_path.open('w', encoding='utf-8') as f:
        yaml.dump(schema_export_data, f, allow_unicode=True, sort_keys=False)
    print(f"[成功] スキーマ情報を '{yaml_path}' にエクスポートしました。")

if __name__ == '__main__':
    export_all_formats_from_folder()