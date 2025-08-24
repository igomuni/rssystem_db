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
import argparse

# --- (パス解決、設定読み込み、API設定は変更なし) ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError: PROJECT_ROOT = Path().cwd()
SETTINGS_FILE = PROJECT_ROOT / 'project_settings.json'

def load_settings():
    try:
        with SETTINGS_FILE.open('r', encoding='utf-8') as f: return json.load(f)
    except Exception as e: sys.exit(f"[エラー] 設定ファイル '{SETTINGS_FILE}' 読込失敗: {e}")

def configure_genai():
    load_dotenv(dotenv_path=PROJECT_ROOT / '.env')
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key: sys.exit("[エラー] .envにGOOGLE_API_KEYがありません。")
    genai.configure(api_key=api_key)

def fetch_and_embed_data(db_file_path: Path, cache_path: Path):
    """【ステップ1】DBからデータを取得し、Embeddingを行い、結果をキャッシュする"""
    print(f"--- データベース '{db_file_path}' からテキストデータを読み込み、文脈を付与します ---")
    con = duckdb.connect(database=str(db_file_path), read_only=True)
    query = """
    SELECT d.予算事業ID, o.事業名, g.事業の目的, g.事業の概要, d.契約概要, d.支出先名, d.金額
    FROM "支出先_支出情報_明細" AS d
    LEFT JOIN "基本情報_組織情報" AS o ON d.予算事業ID = o.予算事業ID
    LEFT JOIN "基本情報_事業概要等" AS g ON d.予算事業ID = g.予算事業ID
    WHERE d.契約概要 IS NOT NULL AND o.事業名 IS NOT NULL AND d.支出先名 IS NOT NULL
    AND LENGTH(d.契約概要) > 5
    AND d.契約概要 NOT IN ('その他', '職員旅費', '謝金', '配分', '電気料', '委託費', 'その他経費');
    """
    df = con.execute(query).fetchdf()
    con.close()
    print(f" -> {len(df)}件の文脈強化済みテキストデータを取得しました。")

    print("--- 文脈強化済みテキストから「説明文」を生成中... ---")
    def create_document(row):
        purpose = row['事業の目的'] if pd.notna(row['事業の目的']) else "目的の記述なし"
        outline = row['事業の概要'] if pd.notna(row['事業の概要']) else "概要の記述なし"
        return (
            f"事業名「{row['事業名']}」。この事業の目的は「{purpose}」であり、概要は「{outline}」です。"
            f"この事業において、支出先「{row['支出先名']}」に対し、「{row['契約概要']}」という内容で契約が行われました。"
        )
    documents = df.apply(create_document, axis=1).tolist()
    df['document_text'] = documents # 後で使えるようにDFにも追加

    print("--- テキストを意味ベクトルに変換中 (Embedding)... ---")
    try:
        def chunks(lst, n):
            for i in range(0, len(lst), n): yield lst[i:i + n]
        
        embeddings_list = []
        for batch in tqdm(list(chunks(documents, 100)), desc="Embedding Progress"):
            result = genai.embed_content(model="models/text-embedding-004", content=batch, task_type="RETRIEVAL_DOCUMENT")
            embeddings_list.extend(result['embedding'])
        
        # Embedding結果をDataFrameの新しい列として追加
        df['embedding'] = embeddings_list

    except Exception as e:
        sys.exit(f"[エラー] Google AI Embedding APIの呼び出しに失敗しました: {e}")
    
    # --- 結果をParquetファイルにキャッシュとして保存 ---
    print(f"--- Embedding結果をキャッシュファイル '{cache_path}' に保存中... ---")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path)
    print("[成功] キャッシュの作成が完了しました。")
    return df

def store_to_chromadb(df_with_embeddings: pd.DataFrame, vector_store_path: str):
    """【ステップ2】キャッシュからデータを読み込み、クリーニングしてChromaDBに保存する"""
    print("--- ベクトルデータベースに保存中... ---")
    
    # --- データクリーニング ---
    # ChromaDBはNone/NaNをメタデータとして受け付けないため、安全な値に置換
    # to_dictの前にクリーニングを行う
    metadatas_df = df_with_embeddings.drop(columns=['document_text', 'embedding'])
    # 文字列のカラムは空文字に、数値のカラムは0に置換
    for col in metadatas_df.select_dtypes(include=['object']).columns:
        metadatas_df[col] = metadatas_df[col].fillna('')
    for col in metadatas_df.select_dtypes(include=['number']).columns:
        metadatas_df[col] = metadatas_df[col].fillna(0)
        
    metadatas = metadatas_df.to_dict('records')
    documents = df_with_embeddings['document_text'].tolist()
    embeddings = df_with_embeddings['embedding'].tolist()
    ids = [f"doc_{i}" for i in range(len(documents))]

    try:
        client = chromadb.PersistentClient(path=vector_store_path)
        if "rs_system_rag" in [c.name for c in client.list_collections()]:
            client.delete_collection(name="rs_system_rag")
        collection = client.create_collection(name="rs_system_rag")
        
        batch_size = 4000
        for i in tqdm(range(0, len(documents), batch_size), desc="Storing to ChromaDB"):
            collection.add(
                ids=ids[i:i + batch_size],
                embeddings=embeddings[i:i + batch_size],
                documents=documents[i:i + batch_size],
                metadatas=metadatas[i:i + batch_size]
            )
        print(f"[成功] ベクトルデータベースを '{vector_store_path}' に構築しました。")
    except Exception as e:
        sys.exit(f"[エラー] ChromaDBへの保存に失敗しました: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="テキストデータをベクトル化し、ChromaDBに保存します。")
    parser.add_argument('--use_cache', action='store_true', help="API呼び出しをスキップし、既存のキャッシュファイルからChromaDBを構築します。")
    args = parser.parse_args()

    settings = load_settings()
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    vector_store_path = str(PROJECT_ROOT / "vector_store")
    cache_path = PROJECT_ROOT / "cache" / "embeddings.parquet"

    configure_genai()

    if args.use_cache:
        if not cache_path.is_file():
            sys.exit(f"[エラー] キャッシュファイル '{cache_path}' が見つかりません。まずキャッシュを構築してください。")
        print(f"--- 既存のキャッシュ '{cache_path}' を使用します ---")
        df_cached = pd.read_parquet(cache_path)
        store_to_chromadb(df_cached, vector_store_path)
    else:
        # Embeddingから実行
        df_embedded = fetch_and_embed_data(db_file_path, cache_path)
        # 続けてChromaDBへの保存を実行
        store_to_chromadb(df_embedded, vector_store_path)