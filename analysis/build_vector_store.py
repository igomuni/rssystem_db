import duckdb
import chromadb
import google.generativeai as genai
import pandas as pd
import sys
import json
from pathlib import Path
import os
from dotenv import load_dotenv
from tqdm import tqdm

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

def build_vector_store():
    """
    DuckDBからテキストデータを抽出し、ベクトル化してChromaDBに保存する。
    """
    settings = load_settings()
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    vector_store_path = str(PROJECT_ROOT / "vector_store")

    if not db_file_path.is_file():
        print(f"[エラー] DBファイル '{db_file_path}' が見つかりません。")
        sys.exit(1)
        
    configure_genai()

    print(f"--- データベース '{db_file_path}' からテキストデータを読み込み中... ---")
    con = duckdb.connect(database=str(db_file_path), read_only=True)
    query = """
    SELECT DISTINCT d.予算事業ID, o.事業名, d.契約概要, d.支出先名
    FROM "支出先_支出情報_明細" AS d
    LEFT JOIN "基本情報_組織情報" AS o ON d.予算事業ID = o.予算事業ID
    WHERE d.契約概要 IS NOT NULL AND o.事業名 IS NOT NULL AND d.支出先名 IS NOT NULL;
    """
    try:
        df = con.execute(query).fetchdf()
    finally:
        con.close()
    
    if df.empty:
        print("[警告] 埋め込み対象のテキストデータが見つかりませんでした。")
        return

    print(f" -> {len(df)}件のテキストデータを取得しました。")

    print("--- テキストを意味ベクトルに変換中 (Embedding)... ---")
    documents = (df["事業名"] + "： " + df["契約概要"] + " (支出先: " + df["支出先名"] + ")").tolist()
    
    try:
        # バッチ処理のために、テキストを100件ずつのチャンクに分割
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        embeddings = []
        # text-embedding-004は100件までバッチ処理可能
        for batch in tqdm(list(chunks(documents, 100)), desc="Embedding Progress"):
            result = genai.embed_content(
                model="models/text-embedding-004",
                content=batch,
                task_type="RETRIEVAL_DOCUMENT" # 検索対象のドキュメントであることを示す
            )
            embeddings.extend(result['embedding'])

    except Exception as e:
        print(f"[エラー] Google AI Embedding APIの呼び出しに失敗しました: {e}", file=sys.stderr)
        return
    
    print("--- ベクトルデータベースに保存中... ---")
    try:
        client = chromadb.PersistentClient(path=vector_store_path)
        if "rs_system_rag" in [c.name for c in client.list_collections()]:
            print(" -> 既存のコレクション 'rs_system_rag' を削除しています...")
            client.delete_collection(name="rs_system_rag")
            
        collection = client.create_collection(name="rs_system_rag")

        metadatas = df.to_dict('records')
        ids = [f"doc_{i}" for i in range(len(documents))]
        
        # ChromaDBのバッチサイズ上限より小さい値で分割 (例: 4000)
        batch_size = 4000
        # tqdmを使って進捗バーを表示
        for i in tqdm(range(0, len(documents), batch_size), desc="Storing to ChromaDB"):
            collection.add(
                ids=ids[i:i + batch_size],
                embeddings=embeddings[i:i + batch_size],
                documents=documents[i:i + batch_size],
                metadatas=metadatas[i:i + batch_size]
            )

        print(f"[成功] ベクトルデータベースを '{vector_store_path}' に構築しました。")
    except Exception as e:
        print(f"[エラー] ChromaDBへの保存に失敗しました: {e}", file=sys.stderr)

if __name__ == '__main__':
    build_vector_store()