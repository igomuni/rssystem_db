import duckdb
import json
import yaml
import sys
import pandas as pd
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

def export_all_formats_from_single_db():
    settings = load_settings()
    try:
        db_file_path_str = settings['database']['output_db_file']
    except KeyError as e:
        print(f"[エラー] 設定ファイルに必要なキー {e} がありません。", file=sys.stderr)
        sys.exit(1)

    print(f"--- データベース '{db_file_path_str}' のスキーマ情報をエクスポートします ---")

    db_path = Path(db_file_path_str)
    if not db_path.is_file():
        print(f"\n[エラー] データベースファイル '{db_path}' が見つかりません。")
        sys.exit(1)

    schema_export_data = {}
    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
        
        # ▼▼▼【変更点1】table_name でソートするSQLに変更 ▼▼▼
        # これにより、出力されるJSON/YAMLのキーが昇順に並びます。
        index_df = con.execute("SELECT * FROM table_index ORDER BY table_name").fetchdf()
        
        print(f"\n{len(index_df)}個のテーブルの情報を処理します...")

        for _, row in index_df.iterrows():
            table_name = row['table_name']
            view_name = row['view_name']
            original_filename = row['original_filename']
            
            schema_df = con.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
            columns_info = schema_df[['name', 'type', 'notnull', 'pk']].to_dict('records')
            
            # ▼▼▼【変更点2】最上位のキーを `table_name` に変更 ▼▼▼
            schema_export_data[table_name] = {
                'view_name': view_name,
                'original_filename': original_filename,
                'columns': columns_info
            }
        
        con.close()
    except Exception as e:
        print(f"\n[エラー] スキーマ情報の取得中にエラーが発生しました: {e}", file=sys.stderr)
        return

    # --- ファイルへの書き出し処理 ---
    # Python 3.7以降の辞書は挿入順序を保持するため、ソートしたまま出力されます。
    # yaml.dumpのsort_keys=Falseも重要です。
    json_path = Path("schema.json")
    with json_path.open('w', encoding='utf-8') as f:
        json.dump(schema_export_data, f, indent=4, ensure_ascii=False)
    print(f"\n[成功] スキーマ情報を '{json_path}' にエクスポートしました。")

    yaml_path = Path("schema.yaml")
    with yaml_path.open('w', encoding='utf-8') as f:
        yaml.dump(schema_export_data, f, allow_unicode=True, sort_keys=False)
    print(f"[成功] スキーマ情報を '{yaml_path}' にエクスポートしました。")

if __name__ == '__main__':
    export_all_formats_from_single_db()