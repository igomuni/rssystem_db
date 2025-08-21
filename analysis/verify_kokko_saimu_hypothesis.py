import duckdb
import pandas as pd
import sys
import json
from pathlib import Path

# --- (パス解決とload_settings関数は変更なし) ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path().cwd()
SETTINGS_FILE = PROJECT_ROOT / 'project_settings.json'

def load_settings():
    # ... (省略)
    try:
        with SETTINGS_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' の読み込みに失敗: {e}", file=sys.stderr)
        sys.exit(1)

def verify_hypothesis():
    """
    「予算超過していない事業」と「国庫債務負担行為の事業」を比較し、
    仮説の妥当性を検証する。
    """
    settings = load_settings()
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    
    if not db_file_path.is_file():
        print(f"[エラー] DBファイル '{db_file_path}' が見つかりません。")
        sys.exit(1)

    print(f"--- データベース '{db_file_path}' を使って仮説を検証します ---")
    con = duckdb.connect(database=str(db_file_path), read_only=True)

    try:
        # --- ステップ1: 「予算超過していない」事業IDのリストを取得 ---
        query_normal_projects = """
        WITH
        BudgetTotal AS (
            SELECT 予算事業ID, SUM("計（歳出予算現額合計）") AS budget_total
            FROM "予算・執行_サマリ"
            GROUP BY 予算事業ID
        ),
        ExpenditureTotal AS (
            SELECT 予算事業ID, SUM("金額") AS expenditure_total
            FROM "支出先_支出情報"
            WHERE "金額" IS NOT NULL
            GROUP BY 予算事業ID
        )
        SELECT b.予算事業ID
        FROM BudgetTotal AS b JOIN ExpenditureTotal AS e ON b.予算事業ID = e.予算事業ID
        WHERE e.expenditure_total <= b.budget_total;
        """
        print("  - 分析中: 予算超過していない事業をリストアップしています...")
        normal_projects_df = con.execute(query_normal_projects).fetchdf()
        normal_project_ids = set(normal_projects_df['予算事業ID'].tolist())
        print(f"    -> {len(normal_project_ids)}件の予算内事業を検出しました。")
        
        # --- ステップ2: 「国庫債務負担行為」の事業IDリストを取得 ---
        query_kokko_saimu = 'SELECT DISTINCT 予算事業ID FROM "支出先_国庫債務負担行為等による契約"'
        print("  - 分析中: 国庫債務負担行為の対象事業をリストアップしています...")
        kokko_saimu_df = con.execute(query_kokko_saimu).fetchdf()
        kokko_saimu_ids = set(kokko_saimu_df['予算事業ID'].tolist())
        print(f"    -> {len(kokko_saimu_ids)}件の国庫債務負担行為事業を検出しました。")

        # --- ステップ3: 2つのリストを比較し、重複（例外）を見つける ---
        print("  - 分析中: 2つのリストを比較し、例外を探しています...")
        # 予算内事業のIDセット と 国庫債務負担行為のIDセット の積集合を取る
        exceptions = normal_project_ids.intersection(kokko_saimu_ids)
        
        con.close()
        
        # --- 結果の報告 ---
        print("\n" + "="*50)
        print("                 仮説の最終検証結果")
        print("="*50)
        print(f"  - 全事業のうち、予算内に収まっている事業数: {len(normal_project_ids)}件")
        print(f"  - 全事業のうち、国庫債務負担行為に登録がある事業数: {len(kokko_saimu_ids)}件")
        print("\n---")
        
        if not exceptions:
            print("\n[結論] 仮説は完全に証明されました！")
            print("予算を超過していない事業の中に、国庫債務負担行為に登録されているものは1件もありませんでした。")
            print("これは、「国庫債務負担行為であること」と「見かけ上、予算を超過すること」の間に、極めて強い相関関係があることを示しています。")
        else:
            print(f"\n[結論] 仮説はほぼ正しいですが、{len(exceptions)}件の例外が見つかりました。")
            print("予算内に収まっているにもかかわらず、国庫債務負担行為として登録されている事業が存在します。")
            print("これらは、契約総額が単年度予算内に収まる小規模な複数年度契約や、会計処理上特殊なケースである可能性が考えられます。")
            print("\n--- 例外リスト (先頭10件) ---")
            for i, ex_id in enumerate(list(exceptions)[:10]):
                print(f"  - 予算事業ID: {ex_id}")
            if len(exceptions) > 10:
                print(f"  ...他 {len(exceptions)-10}件")

        print("="*50)

    except Exception as e:
        print(f"\n[エラー] 分析中にエラーが発生しました: {e}")
        if 'con' in locals() and con:
            con.close()
        return

if __name__ == '__main__':
    verify_hypothesis()