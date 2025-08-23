import duckdb
import pandas as pd
import sys
import json
from pathlib import Path

# --- プロジェクトルートを基準にパスを解決 ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # 対話モードなどで __file__ が未定義の場合
    PROJECT_ROOT = Path().cwd()

SETTINGS_FILE = PROJECT_ROOT / 'project_settings.json'
# ---------------------------------------------

def load_settings():
    """
    プロジェクトルートにある設定ファイルを読み込み、設定内容の辞書を返す
    """
    try:
        with SETTINGS_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' の読み込みに失敗: {e}", file=sys.stderr)
        sys.exit(1)

def extract_unique_text_lists():
    """
    指定されたカラムから、ユニークなテキストリストを抽出し、CSVファイルとして出力する。
    """
    settings = load_settings()
    try:
        db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
        results_folder = PROJECT_ROOT / settings['query_runner']['results_folder']
    except KeyError as e:
        print(f"[エラー] 設定ファイルに必要なキー {e} がありません。", file=sys.stderr)
        sys.exit(1)
    
    if not db_file_path.is_file():
        print(f"[エラー] DBファイル '{db_file_path}' が見つかりません。`import_zips_to_duckdb.py` を実行してください。")
        sys.exit(1)

    print(f"--- データベース '{db_file_path}' からテキストデータを抽出します ---")
    
    try:
        con = duckdb.connect(database=str(db_file_path), read_only=True)
    except Exception as e:
        print(f"[エラー] DB接続に失敗しました: {e}", file=sys.stderr)
        return

    # 抽出対象のカラムと、それが含まれるVIEW名を定義
    targets = {
        '事業名': '"基本情報_事業概要等"',
        '支出先名': '"支出先_支出情報_明細"',
        '契約概要': '"支出先_支出情報_明細"'
    }

    # 出力先フォルダを準備
    results_folder.mkdir(parents=True, exist_ok=True)

    for column_name, view_name in targets.items():
        print(f"\n--- カラム: '{column_name}' のユニークなリストを抽出中... ---")
        try:
            # DISTINCTを使って重複を除いたリストを取得
            query = f'SELECT DISTINCT "{column_name}" FROM {view_name} WHERE "{column_name}" IS NOT NULL;'
            
            df = con.execute(query).fetchdf()
            
            # 結果をCSVファイルに出力
            output_filename = f"unique_list_{column_name}.csv"
            output_path = results_folder / output_filename
            
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            print(f" -> {len(df)}件のユニークなリストを '{output_path}' に保存しました。")

        except Exception as e:
            print(f"    [エラー] '{column_name}' の処理中にエラーが発生しました: {e}")

    con.close()
    print("\n[成功] すべてのテキストデータの抽出が完了しました。")


if __name__ == '__main__':
    extract_unique_text_lists()