import json
import pandas as pd
import os

# --- 設定 ---
# スクリプトと同じ階層にあるJSONファイルを指定
SCHEMA_JSON_FILE = "schema.json"
OUTPUT_CSV_FILE = "schema_from_json.csv"
# --- ここまで ---

def convert_schema_json_to_csv():
    """
    プロジェクトルートにある 'schema.json' を読み込み、
    全テーブルのスキーマ定義を単一のCSVファイルにエクスポートします。
    """
    if not os.path.exists(SCHEMA_JSON_FILE):
        print(f"エラー: スキーマファイル '{SCHEMA_JSON_FILE}' が見つかりません。")
        print("スクリプトがプロジェクトのルートディレクトリで実行されているか確認してください。")
        return

    try:
        # JSONファイルを読み込む
        with open(SCHEMA_JSON_FILE, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
        print(f"スキーマファイル '{SCHEMA_JSON_FILE}' を読み込みました。")
        
        # CSVに出力するための各行のデータを格納するリスト
        all_records = []

        # JSONデータの各テーブル定義をループ処理
        for table_key, table_info in schema_data.items():
            view_name = table_info.get('view_name', '')
            original_filename = table_info.get('original_filename', '')
            
            # 各テーブルの各カラムをループ処理
            for column_info in table_info.get('columns', []):
                # 1つのカラム情報を1行のレコードとして作成
                record = {
                    'table_key': table_key,
                    'view_name': view_name,
                    'original_filename': original_filename,
                    'column_name': column_info.get('name', ''),
                    'column_type': column_info.get('type', ''),
                    'notnull': column_info.get('notnull', False),
                    'pk': column_info.get('pk', False)
                }
                all_records.append(record)

        if not all_records:
            print("スキーマ情報が見つかりませんでした。JSONファイルの中身を確認してください。")
            return

        # レコードのリストをPandas DataFrameに変換
        df = pd.DataFrame(all_records)
        
        # DataFrameのカラム名を日本語にリネーム
        df.columns = [
            "テーブルキー", "ビュー名(テーブル名)", "元ファイル名",
            "カラム名", "データ型", "NULL許容しない", "主キー"
        ]

        # DataFrameをCSVファイルに出力
        df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
        
        print(f"\n[成功] スキーマ定義全体を '{OUTPUT_CSV_FILE}' にエクスポートしました。")

    except json.JSONDecodeError:
        print(f"エラー: '{SCHEMA_JSON_FILE}' は有効なJSON形式ではありません。")
    except Exception as e:
        print(f"\n予期せぬエラーが発生しました: {e}")

# スクリプトが直接実行された場合にのみ関数を呼び出す
if __name__ == "__main__":
    convert_schema_json_to_csv()