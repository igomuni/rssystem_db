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
    except FileNotFoundError:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' が見つかりません。", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' のJSON形式が正しくありません。", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[エラー] 設定ファイルの読み込み中に予期せぬ問題が発生しました: {e}", file=sys.stderr)
        sys.exit(1)

def analyze_project_balance():
    """
    事業ごとの予算・支出バランスをチェックし、超過原因を推定する。
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

    print(f"--- データベース '{db_file_path}' の事業ごと予算・支出バランスを分析します ---")
    
    try:
        con = duckdb.connect(database=str(db_file_path), read_only=True)
    except Exception as e:
        print(f"[エラー] DB接続に失敗しました: {e}", file=sys.stderr)
        return

    # --- ステップ1: 予算超過リストを作成 ---
    base_query = """
    WITH
    BudgetTotal AS (
        SELECT
            予算事業ID, 事業名,
            SUM("計（歳出予算現額合計）") AS "歳出予算現額合計"
        FROM "予算・執行_サマリ"
        GROUP BY 予算事業ID, 事業名
    ),
    ExpenditureTotal AS (
        SELECT 予算事業ID, SUM("金額") AS 事業全体の支出総額
        FROM "支出先_支出情報"
        WHERE "金額" IS NOT NULL
        GROUP BY 予算事業ID
    )
    SELECT
        b.予算事業ID, b.事業名,
        b."歳出予算現額合計" AS "実質的な予算総額",
        e.事業全体の支出総額,
        (e.事業全体の支出総額 - b."歳出予算現額合計") AS "超過額"
    FROM BudgetTotal AS b JOIN ExpenditureTotal AS e ON b.予算事業ID = e.予算事業ID
    WHERE e.事業全体の支出総額 > b."歳出予算現額合計"
    """
    print("  - チェック中: 支出総額が予算総額を超過している事業...")
    
    try:
        issue_df = con.execute(base_query).fetchdf()
    except Exception as e:
        print(f"    [エラー] 予算超過リストの作成中にクエリエラーが発生しました: {e}", file=sys.stderr)
        con.close()
        return
    
    if issue_df.empty:
        print("\n[成功] 予算超過の事業は見つかりませんでした。")
        con.close()
        return

    print(f"    -> {len(issue_df)}件の予算超過が疑われる事業を検出しました。")
    
    # --- ステップ2: 超過原因を推定するための情報を収集 ---
    issue_ids = tuple(issue_df['予算事業ID'].tolist())
    
    try:
        # 国庫債務負担行為の対象IDを取得
        kokko_saimu_ids_df = con.execute(f'SELECT DISTINCT 予算事業ID FROM "支出先_国庫債務負担行為等による契約" WHERE 予算事業ID IN {issue_ids}').fetchdf()
        kokko_saimu_ids = set(kokko_saimu_ids_df['予算事業ID'].tolist())

        # 予算項目にマイナスがあるIDを取得
        negative_budget_ids_df = con.execute(f"""
            SELECT DISTINCT 予算事業ID FROM "予算・執行_サマリ"
            WHERE 予算事業ID IN {issue_ids} AND (
                "当初予算（合計）" < 0 OR "補正予算（合計）" < 0 OR "前年度からの繰越し（合計）" < 0 OR "予備費等（合計）" < 0
            )
        """).fetchdf()
        negative_budget_ids = set(negative_budget_ids_df['予算事業ID'].tolist())
    except Exception as e:
        print(f"    [エラー] 超過原因の調査中にクエリエラーが発生しました: {e}", file=sys.stderr)
        kokko_saimu_ids = set()
        negative_budget_ids = set()

    # --- ステップ3: 原因を推定し、結果をDataFrameに追加 ---
    def estimate_cause(row):
        business_id = row['予算事業ID']
        budget_total = row['実質的な予算総額']
        
        if business_id in kokko_saimu_ids:
            return "国庫債務負担行為の可能性"
        if business_id in negative_budget_ids:
            return "収入/返還等（マイナス予算）の可能性"
        if budget_total == 0:
            return "予算額ゼロ（別会計/繰越金等）"
        # 予算額がマイナスの場合も特殊なケースとして分類
        if budget_total < 0:
            return "予算額マイナス（会計上の調整）"
        return "原因不明（要詳細調査）"

    issue_df['超過原因の推定'] = issue_df.apply(estimate_cause, axis=1)
    
    con.close()

    # --- 結果の表示と保存 ---
    print("\n--- 予算超過が疑われる事業リスト (原因推定付き) ---")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(issue_df.head().to_string())
    if len(issue_df) > 5:
        print(f"...他 {len(issue_df) - 5}件")
        
    print("\n--- 原因別サマリー ---")
    print(issue_df['超過原因の推定'].value_counts().to_string())
    print("----------------------")

    output_filename = "project_balance_analysis_report.csv"
    output_path = results_folder / output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    issue_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[成功] 詳細な分析レポートを '{output_path}' に保存しました。")


if __name__ == '__main__':
    analyze_project_balance()