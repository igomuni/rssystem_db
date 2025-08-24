# Colabでの手動RAG分析手順書

## 1. 目的と概要

本ドキュメントは、Google Generative AI等のAPI利用上限に達した場合でも、Google Colaboratory (Colab)と大規模言語モデル(LLM)搭載のAIアシスタント（**思考エンジン**として機能）との対話を通じて、RAG (Retrieval-Augmented Generation) を用いたデータ分析を継続するための手順を記録するものです。

このプロセスは、APIを介した自動処理を「手動」で再現することにより、RAGシステムの内部動作の理解を深め、対話形式でのデバッグを可能にします。また、本手順は**CPU環境**でも十分に機能することが実証されています。

## 2. 事前準備

### 2.1. Google Driveの準備
- `rs_database.duckdb` (DuckDBデータベース)
- `vector_store/` (ChromaDBベクトルストア)
を、自身のGoogle Driveの任意の場所にアップロードしておきます。

### 2.2. AIアシスタントの準備
本プロジェクトの背景を理解し、思考エンジンとして機能できる高度なAIアシスタント（例: Gemini）との対話画面を開いておきます。

### 2.3. CPUとGPUの自動切り替えについて
この手順書で提供されるスクリプトは、Colab環境にGPUが割り当てられているかを自動で判別し、最適なデバイス（GPUまたはCPU）を利用するように設計されています。

GPUを利用すると、特にベクトル化の処理（ステップ2）が高速になりますが、CPU環境でも数分程度で処理は完了します。

#### ColabでGPUを有効にする方法
もし処理速度を上げたい場合は、以下の手順でGPUを有効にできます。（※無料版では利用時間に制限があるためご注意ください）
1.  上部メニューの「ランタイム」をクリック
2.  「ランタイムのタイプを変更」を選択
3.  「ハードウェア アクセラレータ」のドロップダウンから「T4 GPU」を選択して保存

## 3. 分析ワークフロー

### ステップ0：Colab環境構築

まず、Colabノートブック上で分析環境をセットアップします。

#### セル 1: Driveのマウントとリポジトリのクローン
```python
from google.colab import drive
drive.mount('/content/drive')

# リポジトリをクローン
!git clone https://github.com/igomuni/rssystem_db.git /content/rssystem_db
```

#### セル 2: データベースの配置とライブラリのインストール
**注意:** `DRIVE_DB_PATH`と`DRIVE_VECTOR_STORE_PATH`を、ご自身のGoogle Drive上のパスに合わせて書き換えてください。

```python
import os
import sys

PROJECT_ROOT = "/content/rssystem_db"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- ★★★★★ 自身のGoogle Driveのパスに書き換える ★★★★★ ---
DRIVE_DB_PATH = "/content/drive/MyDrive/path/to/your/rs_database.duckdb"
DRIVE_VECTOR_STORE_PATH = "/content/drive/MyDrive/path/to/your/vector_store"
# -------------------------------------------------------------

# データベースファイルをColab環境にコピー
!cp "{DRIVE_DB_PATH}" "{PROJECT_ROOT}/rs_database.duckdb"
!cp -r "{DRIVE_VECTOR_STORE_PATH}" "{PROJECT_ROOT}/"

# 必要なライブラリをインストール
!pip install -q -r "{PROJECT_ROOT}/requirements.txt"
print("環境構築が完了しました。")
```

---

### 【思考エンジンとの対話開始】

ここから、AIアシスタント（思考エンジン）との対話を開始します。各ステップで得られた**出力結果**を思考エンジンに渡し、次の指示（コード）を受け取ります。

---

### ステップ1：データベース構造の理解

RAGの精度を高めるため、思考エンジンに分析対象となるDuckDBのスキーマ（設計図）を最初に理解させます。

#### セル 3: スキーマ確認
```python
import duckdb
import pandas as pd

DB_FILE = "/content/rssystem_db/rs_database.duckdb"
TARGET_TABLES = [
    "基本情報_事業概要等",
    "支出先_支出情報_明細"
]

try:
    con = duckdb.connect(database=DB_FILE, read_only=True)
    print(f"データベース '{DB_FILE}' に接続し、主要テーブルのスキーマを詳細に確認します。")

    for table_name in TARGET_TABLES:
        print(f"\n--- テーブル '{table_name}' のスキーマ詳細 ---")
        schema_df = con.sql(f'DESCRIBE "{table_name}";').to_df()
        schema_df.columns = ["カラム名", "データ型", "Null許容", "主キー", "デフォルト値", "その他"]
        print(schema_df.to_markdown(index=False))

    con.close()

except Exception as e:
    print(f"\nエラーが発生しました: {e}")
```
> **思考エンジンへの指示:**
> このセルの**出力結果（2つのテーブルスキーマ情報）**をすべてコピーし、「これがデータベースの設計図です」と伝えてください。

### ステップ2：ベクトル検索によるコンテキスト情報の取得 (Retrieval)

自然言語の質問をベクトル化し、関連性の高い事業IDをベクトルDBから検索します。

#### セル 4: ベクトル検索
```python
import sys
import torch
import chromadb
from sentence_transformers import SentenceTransformer
import pandas as pd

# --- 設定 ---
PROJECT_ROOT = "/content/rssystem_db"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
CHROMA_DB_PATH = f"{PROJECT_ROOT}/vector_store"
COLLECTION_NAME = "rs_system_rag"
EMBEDDING_MODEL_NAME = 'sentence-transformers/paraphrase-multilingual-mpnet-base-v2'

# --- ★★★★★ ここに分析したい質問を入力 ★★★★★ ---
question = "ガソリン減税に関連しそうな事業の、支出先トップ3とその金額は？"
# ------------------------------------------------

try:
    # CPU/GPUを自動で判別
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    print(f"使用するデバイス: {device.upper()}")
    
    model = SentenceTransformer(EMBEDDING_MODEL_NAME, device=device)
    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
    collection = client.get_collection(name=COLLECTION_NAME)
    
    question_embedding = model.encode(question).tolist()
    results = collection.query(
        query_embeddings=[question_embedding],
        n_results=10,
        include=["metadatas"]
    )
    print("\nベクトル検索が完了しました。")

    if not results["ids"][0]:
        print("関連する情報が見つかりませんでした。")
    else:
        df = pd.DataFrame({
            '予算事業ID': [meta.get('予算事業ID', 'N/A') for meta in results['metadatas'][0]],
        })
        unique_ids = df['予算事業ID'].unique().tolist()
        
        print(f"\n質問に関連する可能性のある事業IDが {len(unique_ids)}件 見つかりました。")
        print("これらのIDを基に、データベースへ問い合わせるSQLを生成します。")
        print("\n--- コンテキスト情報 (ユニークな事業IDリスト) ---")
        print(unique_ids)

except Exception as e:
    print(f"\nエラーが発生しました: {e}")
```
> **思考エンジンへの指示:**
> このセルの**出力結果（ユニークな事業IDのリスト）**をコピーし、「このコンテキスト情報を基に、元の質問に答えるためのSQLクエリを生成してください」と依頼してください。

### ステップ3：SQLの実行とデータ取得

思考エンジンが生成したSQLクエリをDuckDBで実行し、最終回答の根拠となるデータを取得します。

#### セル 5: SQL実行
```python
import duckdb
import pandas as pd

DB_FILE = "/content/rssystem_db/rs_database.duckdb"

# --- ★★★★★ 思考エンジンが生成したSQLクエリをここに貼り付け ★★★★★ ---
sql_query = """
SELECT
    "支出先名",
    SUM("金額") AS "合計支出額"
FROM
    "支出先_支出情報_明細"
WHERE
    "予算事業ID" IN (4212, 20314, 5145, 5261, 5192, 7519, 7375, 5266, 20091, 5115)
GROUP BY
    "支出先名"
ORDER BY
    "合計支出額" DESC
LIMIT 3;
"""
# -----------------------------------------------------------------

try:
    con = duckdb.connect(database=DB_FILE, read_only=True)
    result_df = con.sql(sql_query).to_df()
    con.close()

    if not result_df.empty:
        result_df["合計支出額"] = result_df["合計支出額"].apply(lambda x: f"{x:,.0f}")
    
    print("--- SQL実行結果 ---")
    print(result_df.to_markdown(index=False))

except Exception as e:
    print(f"\nエラーが発生しました: {e}")
```
> **思考エンジンへの指示:**
> このセルの**出力結果（Markdown形式のテーブル）**をコピーし、「このデータを基に、元の質問に対する最終的な回答を自然な日本語で生成してください」と依頼してください。

### ステップ4：最終回答の取得 (Generation)

思考エンジンが、SQLの実行結果を解釈し、ユーザーにとって分かりやすい最終回答を生成します。この回答をもって、一連の分析は完了です。