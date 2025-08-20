import duckdb
import pandas as pd
import zipfile
import json
import sys
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

def import_zips_to_single_db():
    settings = load_settings()
    try:
        zip_folder_path = settings['database']['input_zip_folder']
        output_db_file = Path(settings['database']['output_db_file'])
    except KeyError as e:
        print(f"[エラー] 設定ファイルに必要なキー {e} がありません。", file=sys.stderr)
        sys.exit(1)
    
    print(f"処理を開始します。出力DBファイル: '{output_db_file}'")

    if output_db_file.exists():
        output_db_file.unlink()

    con = duckdb.connect(database=str(output_db_file), read_only=False)
    
    index_records = []
    # ▼▼▼【変更点1】VIEW名の衝突を避けるために、作成したVIEW名を記録するセット▼▼▼
    created_views = set()

    zip_files_list = list(Path(zip_folder_path).glob('*.zip'))
    if not zip_files_list:
        print(f"エラー: フォルダ '{zip_folder_path}' 内にZIPファイルが見つかりませんでした。")
        return

    print(f"{len(zip_files_list)}個のZIPファイルを検出しました。")

    for zip_path in zip_files_list:
        try:
            # pathlib.stem を使って拡張子を除いたファイル名を取得 (例: 1-1_RS_2024_基本情報_組織情報)
            base_name = zip_path.stem
            parts = base_name.split('_')

            # ▼▼▼【変更点2】新しい命名規則のロジック▼▼▼
            # テーブル名: '1-1' -> 'tbl_1_1'
            id_part = parts[0].replace('-', '_')
            table_name = f'tbl_{id_part}'
            
            # VIEW名: '基本情報_組織情報'
            # ファイル名の3番目の '_' 以降を結合
            description_parts = parts[3:]
            view_name_raw = "_".join(description_parts)

            # VIEW名の衝突回避処理
            view_name = view_name_raw
            counter = 2
            while view_name in created_views:
                view_name = f"{view_name_raw}_{counter}"
                counter += 1
            created_views.add(view_name)
            # ▲▲▲ ここまで ▲▲▲
            
            print(f"\n処理中: '{zip_path.name}' -> テーブル: '{table_name}', VIEW: '{view_name}'")
            
            with zipfile.ZipFile(zip_path, 'r') as zf:
                csv_filename_in_zip = zf.namelist()[0]
                with zf.open(csv_filename_in_zip) as csv_file:
                    try:
                        df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False)
                    except UnicodeDecodeError:
                        csv_file.seek(0)
                        df = pd.read_csv(csv_file, encoding='shift_jis', low_memory=False)
            
            con.from_df(df).create(table_name)
            print(f" -> テーブル '{table_name}' に {len(df):,} 行をインポートしました。")

            con.execute(f"CREATE OR REPLACE VIEW \"{view_name}\" AS SELECT * FROM {table_name};")
            # 日本語のVIEW名を "" で囲むことで、より安全に作成
            print(f" -> VIEW '{view_name}' を作成しました。")
            
            index_records.append({
                'table_name': table_name,
                'view_name': view_name,
                'original_filename': zip_path.name
            })

        except Exception as e:
            print(f" !! エラー: ファイル '{zip_path.name}' の処理中にエラー: {e}", file=sys.stderr)

    if index_records:
        print("\nインデックス用テーブル 'table_index' を作成します...")
        index_df = pd.DataFrame(index_records)
        con.from_df(index_df).create("table_index")
        print(" -> 'table_index' を作成しました。")

    con.close()
    print(f"\nすべての処理が完了しました。データは '{output_db_file}' に保存されています。")

if __name__ == '__main__':
    import_zips_to_single_db()