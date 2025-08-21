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
    created_views = set()

    zip_files_list = list(Path(zip_folder_path).glob('*.zip'))
    if not zip_files_list:
        print(f"エラー: フォルダ '{zip_folder_path}' 内にZIPファイルが見つかりませんでした。")
        con.close()
        return

    print(f"{len(zip_files_list)}個のZIPファイルを検出しました。")

    for zip_path in zip_files_list:
        try:
            base_name_zip = zip_path.name
            base_name = zip_path.stem
            parts = base_name.split('_')

            # --- 通常のテーブル・VIEW名生成ロジック (全ファイル共通) ---
            id_part = parts[0].replace('-', '_')
            table_name = f'tbl_{id_part}'
            
            description_parts = parts[3:]
            view_name_raw = "_".join(description_parts)

            view_name = view_name_raw
            counter = 2
            while view_name in created_views:
                view_name = f"{view_name_raw}_{counter}"
                counter += 1
            created_views.add(view_name)
            
            print(f"\n処理中: '{base_name_zip}' -> テーブル: '{table_name}', VIEW: '{view_name}'")
            
            # --- CSVの読み込み ---
            with zipfile.ZipFile(zip_path, 'r') as zf:
                csv_filename_in_zip = zf.namelist()[0]
                with zf.open(csv_filename_in_zip) as csv_file:
                    try:
                        df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False)
                    except UnicodeDecodeError:
                        csv_file.seek(0)
                        df = pd.read_csv(csv_file, encoding='shift_jis', low_memory=False)
            
            # --- 元テーブルの作成 (全ファイル共通) ---
            con.from_df(df.copy()).create(table_name)
            print(f" -> 元テーブル '{table_name}' に {len(df):,} 行をインポートしました。")

            con.execute(f"CREATE OR REPLACE VIEW \"{view_name}\" AS SELECT * FROM {table_name};")
            print(f" -> 元VIEW '{view_name}' を作成しました。")
            
            index_records.append({
                'table_name': table_name,
                'view_name': view_name,
                'original_filename': base_name_zip
            })

            # ▼▼▼【変更点】'5-1'のファイルの場合のみ、追加の分割処理を実行▼▼▼
            if "5-1_RS_2024_支出先_支出情報.zip" in base_name_zip:
                print(f" -> 追加処理: '{base_name_zip}' をサマリーと明細に分割します。")
                
                # 「金額」列を数値に変換し、NaNを基準に分割
                df['金額'] = pd.to_numeric(df['金額'], errors='coerce')
                
                df_summary = df[df['金額'].isna()].copy()
                df_details = df[df['金額'].notna()].copy()
                
                # --- サマリーテーブルの作成 ---
                summary_table_name = "tbl_5_1_summary"
                summary_view_name = "支出先_支出情報_サマリー"
                con.from_df(df_summary).create(summary_table_name)
                con.execute(f'CREATE OR REPLACE VIEW "{summary_view_name}" AS SELECT * FROM {summary_table_name};')
                print(f"    -> 分割テーブル '{summary_table_name}' ({len(df_summary)}行) と VIEW '{summary_view_name}' を作成。")
                index_records.append({'table_name': summary_table_name, 'view_name': summary_view_name, 'original_filename': base_name_zip})

                # --- 明細テーブルの作成 ---
                details_table_name = "tbl_5_1_details"
                details_view_name = "支出先_支出情報_明細"
                con.from_df(df_details).create(details_table_name)
                con.execute(f'CREATE OR REPLACE VIEW "{details_view_name}" AS SELECT * FROM {details_table_name};')
                print(f"    -> 分割テーブル '{details_table_name}' ({len(df_details)}行) と VIEW '{details_view_name}' を作成。")
                index_records.append({'table_name': details_table_name, 'view_name': details_view_name, 'original_filename': base_name_zip})

        except Exception as e:
            print(f" !! エラー: ファイル '{zip_path.name}' の処理中にエラー: {e}", file=sys.stderr)

    # --- インデックス用テーブルの作成 ---
    if index_records:
        print("\nインデックス用テーブル 'table_index' を作成します...")
        index_df = pd.DataFrame(index_records)
        con.from_df(index_df).create("table_index")
        print(" -> 'table_index' を作成しました。")

    con.close()
    print(f"\nすべての処理が完了しました。データは '{output_db_file}' に保存されています。")

if __name__ == '__main__':
    import_zips_to_single_db()