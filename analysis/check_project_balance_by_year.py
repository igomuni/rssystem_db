import duckdb
import pandas as pd
import sys
import json
from pathlib import Path
import argparse

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

def check_balance_by_year(target_year: int):
    """
    指定された「予算年度」に絞って、予算総額と支出総額を比較する。
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

    print(f"--- 【{target_year}年度予算】を基準に、予算・支出バランスをチェックします ---")
    
    try:
        con = duckdb.connect(database=str(db_file_path), read_only=True)
    except Exception as e:
        print(f"[エラー] DB接続に失敗しました: {e}", file=sys.stderr)
        return

    # 指定された年度の予算と、支出総額を比較するSQLクエリ
    query = f"""
    WITH
    BudgetForYear AS (
        -- 指定された単一年度の予算額合計を取得
        SELECT
            予算事業ID,
            事業名,
            SUM("計（歳出予算現額合計）") AS "単年度の予算総額"
        FROM "予算・執行_サマリ"
        WHERE 予算年度 = {target_year}
        GROUP BY 予算事業ID, 事業名
    ),
    ExpenditureTotal AS (
        -- 支出総額を取得 (これは年度を区別できないが、最新年度の実績と仮定)
        SELECT
            予算事業ID,
            SUM("金額") AS 事業全体の支出総額
        FROM "支出先_支出情報"
        WHERE "金額" IS NOT NULL
        GROUP BY 予算事業ID
    )
    SELECT
        b.予算事業ID,
        b.事業名,
        b."単年度の予算総額",
        e.事業全体の支出総額,
        (e.事業全体の支出総額 - b."単年度の予算総額") AS "超過額"
    FROM
        BudgetForYear AS b
    JOIN
        ExpenditureTotal AS e ON b.予算事業ID = e.予算事業ID
    WHERE
        -- 支出が単年度の予算を超えているものを抽出
        e.事業全体の支出総額 > b."単年度の予算総額"
    ORDER BY
        超過額 DESC;
    """
    
    print(f"  - チェック中: {target_year}年度予算に対し、支出総額が超過している事業...")
    try:
        df = con.execute(query).fetchdf()
        if df.empty:
            print("    -> 矛盾は見つかりませんでした。")
            con.close()
            return
        print(f"    -> {len(df)}件の矛盾が疑われる事業を検出しました。")
    except Exception as e:
        print(f"    [エラー] クエリの実行に失敗しました: {e}")
        con.close()
        return

    con.close()

    print(f"\n--- {target_year}年度予算超過リスト ({len(df)}件) ---")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df)
    print("---------------------------------------------")

    output_filename = f"project_balance_issue_list_{target_year}.csv"
    output_path = results_folder / output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[成功] 詳細なリストを '{output_path}' に保存しました。")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="指定された予算年度の予算と支出総額を比較し、矛盾を検出します。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        'year', 
        type=int,
        default=2024, # ▼▼▼【変更点】デフォルトを2024年度に設定▼▼▼
        nargs='?',    # '?'は引数が0個か1個（省略可能）であることを示す
        help="比較対象とする予算年度 (例: 2023)。\n指定しない場合は、デフォルトで2024年度の予算と比較します。"
    )
    args = parser.parse_args()
    check_balance_by_year(args.year)