import duckdb
import pandas as pd
import argparse
import sys
import json
from pathlib import Path

# --- プロジェクトルートを基準にパスを解決 ---
try:
    # このスクリプトは 'analysis' フォルダ内にあるので、親の親がプロジェクトルート
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
    except FileNotFoundError:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' が見つかりません。", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' のJSON形式が正しくありません。", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[エラー] 設定ファイルの読み込み中に予期せぬ問題が発生しました: {e}", file=sys.stderr)
        sys.exit(1)

def get_details_by_id(business_id: int, db_file_path: str):
    """
    指定された予算事業IDに紐づく全情報を取得し、階層型の辞書として返す
    """
    # ... (この関数の中身は変更ありません) ...
    db_path = Path(db_file_path)
    if not db_path.is_file():
        print(f"[エラー] データベースファイル '{db_file_path}' が見つかりません。", file=sys.stderr)
        return None

    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
    except Exception as e:
        print(f"[エラー] DB接続に失敗: {e}", file=sys.stderr)
        return None

    print(f"--- 予算事業ID: {business_id} の情報を収集中... ---")
    
    try:
        main_info = con.execute(f'SELECT 事業名 FROM "基本情報_組織情報" WHERE 予算事業ID = {business_id} LIMIT 1').fetchdf()
        business_name = main_info['事業名'][0] if not main_info.empty else "不明な事業"
    except Exception:
        business_name = "不明な事業"

    result_json = {
        "予算事業ID": business_id,
        "事業名": business_name,
    }

    views_df = con.execute("SELECT view_name FROM table_index ORDER BY view_name").fetchdf()
    views = views_df['view_name'].tolist()

    for view_name in views:
        print(f"  - VIEW '{view_name}' からデータを取得中...")
        try:
            if '予算事業ID' in [c[0] for c in con.execute(f'DESCRIBE "{view_name}"').fetchall()]:
                df = con.execute(f'SELECT * FROM "{view_name}" WHERE 予算事業ID = {business_id}').fetchdf()
                
                if not df.empty:
                    records = df.to_dict('records')
                    if len(records) == 1:
                        result_json[view_name] = records[0]
                    else:
                        result_json[view_name] = records
            else:
                 print(f"    [情報] VIEW '{view_name}' には予算事業IDがないためスキップします。")

        except Exception as e:
            print(f"    [警告] VIEW '{view_name}' の処理中にエラー: {e}")
    
    con.close()
    return result_json

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="指定された予算事業IDに紐づく全情報をJSON形式で出力します。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('business_id', type=int, help="情報を取得したい予算事業ID (例: 7259)")
    parser.add_argument(
        '-o', '--output', 
        type=str, 
        help="結果を保存するJSONの「ファイル名」(例: details.json)。\n指定しない場合はコンソールに直接出力します。"
    )
    args = parser.parse_args()

    settings = load_settings()
    try:
        db_file = PROJECT_ROOT / settings['database']['output_db_file']
        # ▼▼▼【変更点1】結果出力フォルダのパスを設定から読み込む▼▼▼
        results_folder = PROJECT_ROOT / settings['query_runner']['results_folder']
    except KeyError as e:
        print(f"[エラー] 設定ファイルに必要なキー {e} がありません。", file=sys.stderr)
        sys.exit(1)
    
    final_data = get_details_by_id(args.business_id, str(db_file))

    if final_data:
        json_output = json.dumps(final_data, indent=4, ensure_ascii=False)
        
        if args.output:
            # ▼▼▼【変更点2】出力パスを「結果フォルダ」と「指定ファイル名」で構築▼▼▼
            output_path = results_folder / args.output
            
            # 結果フォルダが存在しない場合は作成
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            output_path.write_text(json_output, encoding='utf-8')
            print(f"\n[成功] 全情報を '{output_path}' に保存しました。")
        else:
            print("\n--- 取得結果 (JSON) ---")
            print(json_output)
            print("------------------------")