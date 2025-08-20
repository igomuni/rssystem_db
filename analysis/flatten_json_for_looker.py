import pandas as pd
import json
from pathlib import Path
import argparse
import sys

# --- プロジェクトルートを基準にパスを解決 ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # 対話モードなどで __file__ が未定義の場合
    PROJECT_ROOT = Path().cwd()

def flatten_json_to_csv(json_file_path: Path, output_csv_path: Path):
    """
    階層化された事業詳細JSONを読み込み、平坦化してCSVに出力する
    """
    print(f"--- JSONファイル '{json_file_path}' を読み込んでいます... ---")
    with json_file_path.open('r', encoding='utf-8') as f:
        data = json.load(f)

    # --- json_normalizeの引数を修正 ---
    try:
        # ▼▼▼【変更点】meta引数から衝突の原因となる '予算事業ID' と '事業名' を削除 ▼▼▼
        # これらの情報は、record_pathで指定した '支出先_支出情報' の中にも
        # 同じものが含まれているため、metaで指定する必要はありません。
        df_flat = pd.json_normalize(
            data,
            record_path=['支出先_支出情報'],
            meta=[
                # ネストしたオブジェクトから値を取り出す場合はリストで階層を指定
                ['基本情報_組織情報', '府省庁']
            ]
        )
    except KeyError as e:
        print(f"\n[エラー] JSONファイルに必要なキー {e} が見つかりませんでした。")
        print("  - '支出先_支出情報' や '基本情報_組織情報' がJSONに含まれているか確認してください。")
        sys.exit(1)
    
    # カラム名を分かりやすく変更
    df_flat = df_flat.rename(columns={
        '基本情報_組織情報.府省庁': '担当府省庁'
    })

    print(f"--- 平坦化されたデータを '{output_csv_path}' に保存します... ---")
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    df_flat.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
    print("[成功] 処理が完了しました。")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="事業詳細JSONをLooker Studio用のCSVに変換します。")
    parser.add_argument('json_file', type=str, help="入力元のJSONファイルパス。 (例: results/business_details_7259.json)")
    parser.add_argument('output_csv', type=str, help="出力先のCSVファイル名。 (例: looker_ready_7259.csv)")
    args = parser.parse_args()

    input_path = PROJECT_ROOT / args.json_file
    # 出力先は `results` フォルダに固定
    output_path = PROJECT_ROOT / "results" / args.output_csv

    if not input_path.is_file():
        print(f"[エラー] 入力ファイル '{input_path}' が見つかりません。")
    else:
        flatten_json_to_csv(input_path, output_path)