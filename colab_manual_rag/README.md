# RSシステム分析プラットフォーム on Google Colab スタートガイド

## 1. 概要

このドキュメントは、日本の行政事業レビューシート（RSシステム）の元データ（ZIP/CSV）から、Google Colaboratory (Colab)上で高度な自然言語分析（RAG）を行うための、完全なセットアップ手順を記したものです。

**最終目標:**
ローカルPCでデータ基盤を構築し、その成果物をGoogle Drive経由でColabに持ち込み、対話型AIアシスタントと協力して、自然言語でのデータベース分析を実現します。

## 2. 【Phase 1】 ローカルPCでのデータ基盤構築

このフェーズは、分析の土台となる2つの重要なデータベースを、ローカルPC上で生成する作業です。**元データが更新された場合にのみ、再実行が必要**です。

### 2.1. 準備

1.  **プロジェクトのクローン:**
    このプロジェクトのGitHubリポジトリを、ローカルPCにクローンします。
    ```bash
    git clone https://github.com/igomuni/rssystem_db.git
    cd rssystem_db
    ```
2.  **必要なライブラリのインストール:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **元データの配置:**
    `project_settings.json`で定義された入力フォルダ（デフォルト: `download/`）を作成し、[RSシステム](https://rssystem.go.jp/download-csv)からダウンロードしたZIPファイルをすべて格納します。
4.  **APIキーの設定:**
    プロジェクトのルートに`.env`ファイルを作成し、ご自身のGoogle AI APIキーを設定します。
    ```
    GOOGLE_API_KEY="YOUR_API_KEY_HERE"
    ```

### 2.2. DuckDBの構築（構造化DB）

以下のコマンドを実行し、すべてのCSVデータを単一の構造化データベースファイル `rs_database.duckdb` に変換します。
```bash
python import_zips_to_duckdb.py
```

### 2.3. ベクトルDBの構築（RAG用DB）

次に、自然言語検索の土台となるベクトルデータベースを構築します。この処理はAPIを呼び出すため、数十分かかる場合があります。
```bash
python analysis/build_vector_store.py
```
実行が完了すると、プロジェクトルートに `vector_store/` フォルダが生成されます。

## 3. 【Phase 2】 成果物のGoogle Driveへのアップロード

Colabからアクセスできるよう、Phase 1で生成したデータ資産をZIP形式に圧縮し、Google Driveにアップロードします。

### 3.1. 成果物の圧縮

ローカルPCのターミナルで、お使いのOSに応じたコマンドを実行します。

- **macOS / Linux の場合:**
  ```bash
  # rs_database.duckdb を圧縮
  zip rs_database.zip rs_database.duckdb

  # vector_store/ フォルダを圧縮
  zip -r vector_store.zip vector_store
  ```

- **Windows (PowerShell) の場合:**
  ```powershell
  # rs_database.duckdb を圧縮
  Compress-Archive -Path rs_database.duckdb -DestinationPath rs_database.zip

  # vector_store/ フォルダを圧縮
  Compress-Archive -Path vector_store -DestinationPath vector_store.zip
  ```

### 3.2. Google Driveへのアップロード

1.  ご自身のGoogle Drive内に、このプロジェクト専用のフォルダを作成します。（例: `Colab Notebooks/rs_system_data`）
2.  作成した `rs_database.zip` と `vector_store.zip` の2つのファイルを、そのフォルダにアップロードします。

---
---

## 4. 【Phase 3】 Google Colabでの手動RAG分析

ここからは、Google Colabのノートブック上での作業です。API上限時でも、AIアシスタントを「思考エンジン」として活用し、分析を進めます。

### 4.1. CPUとGPUの利用について
この手順書のスクリプトは、Colab環境にGPUが割り当てられているかを自動で判別し、最適なデバイスを利用します。GPUを利用するとベクトル化処理が高速になりますが、**CPU環境でも全く問題なく実行可能**です。

**（任意）GPUを有効にする方法:**
1. Colab上部メニューの「ランタイム」→「ランタイムのタイプを変更」
2. 「ハードウェア アクセラレータ」で「T4 GPU」を選択して保存

### 4.2. Colab環境の初期セットアップ

新しいColabノートブックを作成し、**最初のコードセル**に以下の内容を貼り付けて実行します。`DRIVE_DATA_PATH`は、ご自身の環境に合わせて修正してください。

```python
# ==================================
#  Colab環境 初期セットアップセル
# ==================================
from google.colab import drive
from pathlib import Path
import os
import sys

# --- 1. Google Driveへの接続 ---
print("--- Step 1: Google Driveへの接続 ---")
drive.mount('/content/drive')

# --- 2. パス設定 (★ご自身の環境に合わせて変更) ---
# ZIPファイル等をアップロードしたGoogle Drive上のフォルダパス
DRIVE_DATA_PATH = Path('/content/drive/MyDrive/Colab Notebooks/rs_system_data')
PROJECT_NAME = 'rssystem_db'
PROJECT_PATH = Path(f'/content/{PROJECT_NAME}')

# --- 3. GitHubからソースコードをクローン ---
print(f"\n--- Step 2: GitHubから '{PROJECT_NAME}' をクローンします ---")
os.system(f'git clone -q https://github.com/igomuni/rssystem_db.git {PROJECT_PATH}')
os.chdir(PROJECT_PATH)
if str(PROJECT_PATH) not in sys.path:
    sys.path.append(str(PROJECT_PATH))

# --- 4. 必要なデータの展開 ---
print(f"\n--- Step 3: '{PROJECT_PATH}' にデータを展開します ---")
os.system(f'unzip -q -o "{DRIVE_DATA_PATH / "rs_database.zip"}" -d "{PROJECT_PATH}"')
os.system(f'unzip -q -o "{DRIVE_DATA_PATH / "vector_store.zip"}" -d "{PROJECT_PATH}"')

# --- 5. ライブラリのインストール ---
print("\n--- Step 4: ライブラリインストール ---")
os.system('pip install -q -r requirements.txt')

# --- 6. 最終確認 ---
print("\n--- Step 5: 最終的なプロジェクトフォルダの中身を確認します ---")
os.system('ls -l')
print(f"\n[成功] プロジェクト環境の構築が完了しました！")
```

### 4.3. AIアシスタント（思考エンジン）との共同分析

環境が整ったら、AIアシスタントとの対話を開始し、以下の手順で分析を進めます。各ステップで得られた**出力結果**をAIに渡し、次の指示（コード）を受け取ります。
（※プロンプトの詳細は `prompt_examples.md` を参照）

#### **ステップ1：データベース構造の理解**
思考エンジンに分析対象となるDuckDBのスキーマを最初に理解させます。

```python
# セル2：スキーマ確認
import duckdb
DB_FILE = f"{PROJECT_PATH}/rs_database.duckdb"
TARGET_TABLES = ["基本情報_事業概要等", "支出先_支出情報_明細"]

con = duckdb.connect(database=DB_FILE, read_only=True)
for table_name in TARGET_TABLES:
    print(f"\n--- テーブル '{table_name}' のスキーマ詳細 ---")
    schema_df = con.sql(f'DESCRIBE "{table_name}";').to_df()
    schema_df.columns = ["カラム名", "データ型", "Null許容", "主キー", "デフォルト値", "その他"]
    print(schema_df.to_markdown(index=False))
con.close()
```

#### **ステップ2：ベクトル検索によるコンテキスト情報の取得 (Retrieval)**
自然言語の質問に基づき、関連性の高い事業IDをベクトルDBから検索します。

```python
# セル3：ベクトル検索
import torch
import chromadb
from sentence_transformers import SentenceTransformer

# --- ★★★★★ ここに分析したい質問を入力 ★★★★★ ---
question = "ガソリン減税に関連しそうな事業の、支出先トップ3とその金額は？"

# --- 設定 ---
CHROMA_DB_PATH = f"{PROJECT_PATH}/vector_store"
COLLECTION_NAME = "rs_system_rag"
EMBEDDING_MODEL_NAME = 'sentence-transformers/paraphrase-multilingual-mpnet-base-v2'

device = 'cuda' if torch.cuda.is_available() else 'cpu'
print(f"使用するデバイス: {device.upper()}")

model = SentenceTransformer(EMBEDDING_MODEL_NAME, device=device)
client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
collection = client.get_collection(name=COLLECTION_NAME)

question_embedding = model.encode(question).tolist()
results = collection.query(query_embeddings=[question_embedding], n_results=10, include=["metadatas"])

unique_ids = sorted(list(set(meta.get('予算事業ID') for meta in results['metadatas'])))
print("\n--- コンテキスト情報 (ユニークな事業IDリスト) ---")
print(unique_ids)
```

#### **ステップ3：SQLの実行とデータ取得**
思考エンジンが生成したSQLクエリを実行し、最終回答の根拠となるデータを取得します。

```python
# セル4：SQL実行
import duckdb
import pandas as pd
DB_FILE = f"{PROJECT_PATH}/rs_database.duckdb"

# --- ★★★★★ 思考エンジンが生成したSQLクエリをここに貼り付け ★★★★★ ---
sql_query = """
SELECT "支出先名", SUM("金額") AS "合計支出額"
FROM "支出先_支出情報_明細"
WHERE "予算事業ID" IN (4212, 5115, 5145, 5192, 5261, 5266, 7375, 7519, 20091, 20314)
GROUP BY "支出先名" ORDER BY "合計支出額" DESC LIMIT 3;
"""

con = duckdb.connect(database=DB_FILE, read_only=True)
result_df = con.sql(sql_query).to_df()
con.close()

if not result_df.empty:
    result_df["合計支出額"] = result_df["合計支出額"].apply(lambda x: f"{x:,.0f}")

print("--- SQL実行結果 ---")
print(result_df.to_markdown(index=False))
```

#### **ステップ4：最終回答の取得 (Generation)**
AIアシスタントが、SQLの実行結果を解釈し、最終的な回答を生成します。これで分析は完了です。