import duckdb
import chromadb
import google.generativeai as genai
import pandas as pd
import sys
import json
from pathlib import Path
import os
import argparse
from dotenv import load_dotenv

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
        print(f"[エラー] 設定ファイル '{SETTINGS_FILE}' の読み込みに失敗: {e}", file=sys.stderr)
        sys.exit(1)

def configure_genai():
    """
    .envファイルからAPIキーを読み込み、Google AIを設定する
    """
    load_dotenv(dotenv_path=PROJECT_ROOT / '.env')
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("[エラー] .envファイルに GOOGLE_API_KEY が設定されていません。")
        sys.exit(1)
    genai.configure(api_key=api_key)

def rag_text_to_sql(question: str):
    """
    RAGを使って自然言語の質問からSQLを生成し、実行する。
    """
    settings = load_settings()
    db_file_path = str(PROJECT_ROOT / settings['database']['output_db_file'])
    vector_store_path = str(PROJECT_ROOT / "vector_store")
    
    configure_genai()

    print("--- Step 1: ベクトル検索で関連情報を取得中... ---")
    try:
        client = chromadb.PersistentClient(path=vector_store_path)
        collection = client.get_collection(name="rs_system_rag")
    except Exception as e:
        print(f"[エラー] ベクトルDBの読み込みに失敗しました: {e}", file=sys.stderr)
        print(f"  -> まず `analysis/build_vector_store.py` を実行してください。")
        return

    try:
        result = genai.embed_content(
            model="models/text-embedding-004",
            content=question,
            task_type="RETRIEVAL_QUERY"
        )
        query_embedding = result['embedding']
        results = collection.query(query_embeddings=[query_embedding], n_results=10)
    except Exception as e:
        print(f"[エラー] ベクトル検索中にAPIエラーが発生しました: {e}", file=sys.stderr)
        return

    relevant_ids = sorted(list({item['予算事業ID'] for item in results['metadatas'][0]}))
    context_info = "\n".join(f"- {doc}" for doc in results['documents'][0])
    
    print(f" -> 関連性の高い事業ID: {relevant_ids}")
    
    print("--- Step 2: LLMへのプロンプトを構築中... ---")
    try:
        schema_info = (PROJECT_ROOT / "schema.yaml").read_text(encoding='utf-8')
    except FileNotFoundError:
        schema_info = "スキーマ情報なし"
    
    try:
        prompt_template_path = PROJECT_ROOT / "prompts" / "rag_sql_generation_prompt.txt"
        prompt_template = prompt_template_path.read_text(encoding='utf-8')
    except FileNotFoundError:
        print(f"[エラー] プロンプトテンプレート '{prompt_template_path}' が見つかりません。", file=sys.stderr)
        return
    
    prompt = prompt_template.format(
        schema_info=schema_info,
        relevant_ids=relevant_ids,
        context_info=context_info,
        question=question
    )

    print("--- Step 3: LLM (Gemini) にSQLを生成させています... ---")
    try:
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        response = model.generate_content(prompt)
        # LLMの出力から余計な文字を削除
        generated_sql = response.text.strip().replace('```sql', '').replace('```', '')
    except Exception as e:
        print(f"[エラー] LLM APIの呼び出しに失敗しました: {e}", file=sys.stderr)
        return
    
    print(f" -> 生成されたSQL (クリーニング後):\n{generated_sql}\n")
    
    print("--- Step 4: 生成されたSQLをDuckDBで実行しています... ---")
    try:
        con = duckdb.connect(database=db_file_path, read_only=True)
        result_df = con.execute(generated_sql).fetchdf()
        
        print("\n--- 最終的な回答 ---")
        pd.set_option('display.max_rows', 100)
        pd.set_option('display.width', 120)
        print(result_df.to_string())
        print("--------------------")

    except Exception as e:
        print(f" [エラー] 生成されたSQLの実行に失敗しました: {e}", file=sys.stderr)
    finally:
        if 'con' in locals():
            con.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="RAGを使って自然言語の質問からSQLを生成・実行します。")
    parser.add_argument('question', type=str, help="データベースに対する質問 (日本語で自由に入力)")
    args = parser.parse_args()
    
    rag_text_to_sql(args.question)