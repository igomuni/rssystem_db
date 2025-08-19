import duckdb
import pandas as pd
import zipfile
from pathlib import Path

# --- 設定 ---
# ▼▼▼【変更点】ZIPファイルの格納フォルダ名を 'download' に変更▼▼▼
ZIP_FOLDER_PATH = 'download'
OUTPUT_DB_FOLDER = 'duckdb_files'

def import_zips_to_individual_dbs():
    """
    ZIPファイルを個別のDuckDBファイルに1テーブルずつ変換し、
    指定されたフォルダに出力します。
    """
    print(f"処理を開始します。出力先フォルダ: '{OUTPUT_DB_FOLDER}'")

    output_path = Path(OUTPUT_DB_FOLDER)
    output_path.mkdir(exist_ok=True)

    zip_files_list = list(Path(ZIP_FOLDER_PATH).glob('*.zip'))
    if not zip_files_list:
        print(f"エラー: フォルダ '{ZIP_FOLDER_PATH}' 内にZIPファイルが見つかりませんでした。")
        return

    print(f"{len(zip_files_list)}個のZIPファイルを検出しました。")

    for zip_path in zip_files_list:
        try:
            base_name_zip = zip_path.name

            name_parts = base_name_zip.replace('.zip', '').split('_', 3)
            short_name = "_".join(name_parts[:3])
            table_identifier = short_name.replace('-', '_')
            table_name = f'tbl_{table_identifier}'
            
            db_file_path = output_path / f"{table_name}.duckdb"

            print(f"\n処理中: '{base_name_zip}' -> DBファイル: '{db_file_path}'")
            
            if db_file_path.exists():
                db_file_path.unlink()

            con = duckdb.connect(database=str(db_file_path), read_only=False)

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

            meta_df = pd.DataFrame([{'original_filename': base_name_zip}])
            con.from_df(meta_df).create('_metadata')
            print(f" -> メタデータ (_metadata) を作成しました。")
            
            con.close()

        except Exception as e:
            print(f" !! エラー: ファイル '{zip_path.name}' の処理中に問題が発生しました。スキップします。")
            print(f"    詳細: {e}")

    print(f"\nすべての処理が完了しました。データは '{OUTPUT_DB_FOLDER}' フォルダに保存されています。")

if __name__ == '__main__':
    import_zips_to_individual_dbs()