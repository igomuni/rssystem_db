import duckdb
import pandas as pd
import sys
import json
from pathlib import Path
import os
import argparse
from dotenv import load_dotenv
import google.generativeai as genai
from tqdm import tqdm
import time
import re

# --- プロジェクトルートを基準にパスを解決 ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path().cwd()
SETTINGS_FILE = PROJECT_ROOT / 'project_settings.json'
# ---------------------------------------------

def load_settings():
    """設定ファイルを読み込む"""
    try:
        with SETTINGS_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        raise IOError(f"設定ファイル '{SETTINGS_FILE}' の読み込みに失敗しました: {e}")

def configure_genai():
    """APIキーを設定する"""
    load_dotenv(dotenv_path=PROJECT_ROOT / '.env')
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError(".envファイルに GOOGLE_API_KEY が設定されていません。")
    genai.configure(api_key=api_key)

def audit_text_consistency(ministry: str = None, sort_by: str = None, top_n: int = None, sample_size: int = 100, free_tier_safe: bool = False):
    """
    事業名と契約概要の整合性をLLMに監査させ、レポートを出力する。
    """
    settings = load_settings()
    db_file_path = str(PROJECT_ROOT / settings['database']['output_db_file'])
    results_folder = PROJECT_ROOT / settings['query_runner']['results_folder']
    
    configure_genai()
    try:
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
    except Exception as e:
        raise RuntimeError(f"LLMモデルの初期化に失敗しました: {e}")

    # --- パラメータに応じてSQLクエリを動的に構築 ---
    base_query = 'SELECT 事業名, 契約概要, 金額, 府省庁 FROM "支出先_支出情報_明細" WHERE 事業名 IS NOT NULL AND 契約概要 IS NOT NULL'
    
    if ministry:
        base_query += f" AND 府省庁 = '{ministry}'"
        print(f"--- 絞り込み条件: 府省庁 = '{ministry}' ---")

    order_clause = ""
    if sort_by:
        allowed_sort_columns = {"金額", "落札率"}
        if sort_by in allowed_sort_columns:
            order_clause = f' ORDER BY "{sort_by}" DESC'
            print(f"--- 並べ替え条件: {sort_by} の降順 ---")
        else:
            print(f"[警告] 無効なソートキーです: {sort_by}。ソートは無視されます。")
            sort_by = None
    
    limit_clause = ""
    effective_sample_size = 0
    if top_n:
        limit_clause = f" LIMIT {top_n}"
        print(f"--- サンプリング方法: 上位 {top_n}件 ---")
        effective_sample_size = top_n
    else:
        if not sort_by:
            order_clause = " ORDER BY random()"
        limit_clause = f" LIMIT {sample_size}"
        print(f"--- サンプリング方法: ランダムに {sample_size}件 ---")
        effective_sample_size = sample_size

    final_query = base_query + order_clause + limit_clause

    print(f"--- データベースから{effective_sample_size}件のサンプルを抽出中... ---")
    con = duckdb.connect(database=db_file_path, read_only=True)
    try:
        sample_df = con.execute(final_query).fetchdf()
    finally:
        con.close()

    if sample_df.empty:
        print("[警告] 監査対象のデータが見つかりませんでした。条件を確認してください。")
        return

    # --- LLMによる監査ループ ---
    audit_results = []
    print(f"--- LLMによる監査を開始します ({len(sample_df)}件) ---")
    if free_tier_safe:
        print("[情報] 無料API向けの安全モードが有効です (リクエスト間に1秒の待機を入れます)。")
    
    prompt_template = """
あなたは、細部にまで気を配る、経験豊富な政府の会計検査官です。
以下の「事業名」と「契約概要」のペアを評価し、両者の内容が論理的に整合しているかを判断してください。

# 事業名:
{business_name}

# 契約概要:
{contract_outline}

# あなたの評価:
両者の整合性について、1 (全く無関係) から 5 (完全に一致) の5段階でスコアを付け、その理由を簡潔に述べてください。
回答は必ず以下のJSON形式で出力してください。
{{"score": [スコア], "reason": "[理由]"}}
"""

    # ▼▼▼【修正点】sample_dfが定義された関数スコープ内でループ処理を行う▼▼▼
    for _, row in tqdm(sample_df.iterrows(), total=len(sample_df), desc="Auditing Progress"):
        prompt = prompt_template.format(
            business_name=row['事業名'],
            contract_outline=row['契約概要']
        )
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = model.generate_content(prompt)
                json_response = json.loads(response.text.strip().replace("```json", "").replace("```", ""))
                
                audit_results.append({
                    '事業名': row['事業名'],
                    '契約概要': row['契約概要'],
                    '金額': row['金額'],
                    '府省庁': row['府省庁'],
                    'score': json_response.get('score'),
                    'reason': json_response.get('reason')
                })
                break
                
            except Exception as e:
                if free_tier_safe and "429" in str(e):
                    wait_time = 60
                    match = re.search(r'seconds: (\d+)', str(e))
                    if match:
                        wait_time = int(match.group(1)) + 1
                    
                    print(f"\n[情報] レートリミット到達。{wait_time}秒待機して再試行します... ({attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    print(f"\n[警告] LLM APIで予期せぬエラー。スキップします。 Error: {e}")
                    audit_results.append({**row.to_dict(), 'score': 0, 'reason': f'API error: {e}'})
                    break
        else:
            print(f"\n[エラー] {max_retries}回のリトライ失敗。処理を中止。")
            audit_results.append({**row.to_dict(), 'score': 0, 'reason': 'Max retries exceeded'})

        if free_tier_safe:
            time.sleep(1)
    
    result_df = pd.DataFrame(audit_results).sort_values(by='score', ascending=True)
    
    print("\n--- 監査結果サマリー (スコアが低いものから) ---")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 120)
    print(result_df.head(10).to_string())
    print("---------------------------------------------")

    output_filename = f"audit_report_{ministry or 'all'}_{sort_by or 'random'}_{effective_sample_size}.csv"
    output_path = results_folder / output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n[成功] 詳細な監査レポートを '{output_path}' に保存しました。")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="事業名と契約概要の整合性をLLMに監査させます。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('sample_size', type=int, nargs='?', default=100, help="監査するサンプルの件数 (デフォルト: 100)")
    parser.add_argument('-m', '--ministry', type=str, help="監査対象とする府省庁名を指定します。 (例: '財務省')")
    parser.add_argument('-s', '--sort_by', type=str, choices=['金額', '落札率'], help="サンプリング前の並べ替えキーを指定します。 (金額, 落札率)")
    parser.add_argument('-t', '--top_n', type=int, help="ランダムサンプリングの代わりに、ソート後の上位N件を監査対象とします。")
    parser.add_argument(
        '--free_tier_safe', 
        action='store_true',
        help="Google AIの無料枠レートリミットを回避するための安全モードを有効にします (1秒/リクエスト + 自動リトライ)。"
    )
    args = parser.parse_args()
    
    size = args.top_n if args.top_n else args.sample_size
    
    try:
        audit_text_consistency(
            ministry=args.ministry,
            sort_by=args.sort_by,
            top_n=args.top_n,
            sample_size=size,
            free_tier_safe=args.free_tier_safe
        )
    except (IOError, ValueError, RuntimeError) as e:
        print(f"\n[処理中断] {e}")
        sys.exit(1)