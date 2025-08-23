# 特定目的分析スクリプト集

このフォルダには、プロジェクトのデータベース(`rs_database.duckdb`)を利用して、特定のテーマや疑問に答えるための、より高度で応用的な分析スクリプトを格納します。

各スクリプトは、プロジェクトのルートディレクトリから実行することを想定しています。

---

## 1. 自然言語での対話型データベース分析 (RAG System)

**日本語の自然な質問**をするだけで、この複雑な財政データベースから**正確な答え**を引き出せる、インテリジェントな分析システムを構築する。

これを実現するために、**RAG (Retrieval-Augmented Generation)** という最新のAIアーキテクチャを採用しています。システムは、以下の2つのデータベースを連携させて動作します。

1.  **構造化データベース (DuckDB):**
    金額やIDといった、正確な数値データを格納する、プロジェクトのメインDB。
2.  **ベクトルデータベース (ChromaDB):**
    事業名や契約概要といった、テキストデータの「意味」をベクトルとして格納する、意味検索用のDB。

### RAGシステム利用までのワークフロー

この対話型分析システムを利用可能にするには、以下のステップを実行します。

#### Step 1: DuckDBの構築 (CSVから構造化DBへ)

まず、プロジェクトのルートディレクトリにあるスクリプトを使い、ダウンロードしたZIP/CSVデータから、分析の土台となるDuckDBファイルを生成します。

```bash
# プロジェクトのルートディレクトリで実行
python import_zips_to_duckdb.py
```
これにより、`rs_database.duckdb` が作成されます。

#### Step 2: ベクトルDBの構築 (DuckDBからベクトルDBへ)

次に、`analysis/`フォルダ内のスクリプトを使い、DuckDBからテキスト情報を抽出し、意味検索のためのベクトルデータベースを構築します。この処理は、APIを呼び出すため時間がかかりますが、**初回またはDB更新時に一度だけ**行えばOKです。

- **`build_vector_store.py`**
  - **目的:** DuckDBから事業名や契約概要を読み込み、Google AIのEmbeddingモデルで「意味ベクトル」に変換し、ローカルのChromaDBに保存します。
  - **使い方:**
    ```bash
    # (事前に.envファイルにAPIキーの設定が必要です)
    python analysis/build_vector_store.py
    ```
    これにより、`vector_store/` フォルダが作成されます。

#### Step 3: 自然言語での質問 (RAGの実行)

データ基盤の準備が整ったら、いよいよ自然言語でデータベースに質問します。

- **`ask_with_rag.py`**
  - **目的:** ユーザーからの自然な質問をAIが解釈し、まずベクトルDBで関連情報を検索（Retrieval）、その情報を元にLLMが高精度なSQLを生成（Generation）、最終的な答えをDuckDBから導き出します。
  - **使い方:**
    ```bash
    # 「ガソリン減税」に関連する事業の支出先トップ3を調べる
    python analysis/ask_with_rag.py "ガソリン減税に関連しそうな事業の、支出先トップ3とその金額を教えて"

    # 特定の企業への支出について調べる
    python analysis/ask_with_rag.py "博報堂への支出で、金額が大きいものを3つ教えて"
    ```

---

## 2. データ品質と整合性の検証

データベースの健全性をチェックし、分析の信頼性を高めるためのスクリプト群です。

### `generate_data_quality_report.py`

- **目的:**
  データベース全体をスキャンし、「明細行なのに支出先名が空欄」など、データの構造を理解した上で、品質が低い可能性のあるレコードを網羅的にリストアップします。
- **使い方:**
  ```bash
  python analysis/generate_data_quality_report.py
  ```

### `validate_summary_details_split.py`

- **目的:**
  分割した「支出先_支出情報_サマリー」と「支出先_支出情報_明細」の金額が、会計区分をまたいで完全に一致することを検証し、データ分割の正当性を証明します。
- **使い方:**
  ```bash
  python analysis/validate_summary_details_split.py
  ```

### `validate_details_breakdown.py`

- **目的:**
  「支出先_支出情報_明細」の契約額と、「支出先_費目・使途」の内訳合計額の間に、なぜ乖離が発生するのかを調査・検証します。
- **使い方:**
  ```bash
  python analysis/validate_details_breakdown.py
  ```

---

## 3. 予算と支出のバランス分析

事業ごとの予算と支出の関係性を多角的に分析し、データセットの会計上の特性を解き明かすための一連のスクリプトです。

### `check_project_balance_by_year.py`

- **目的:**
  指定された`予算年度`の予算総額と、支出情報テーブルの支出総額を事業ごとに比較し、「見かけ上の予算超過」が発生している事業をリストアップします。
- **使い方:**
  ```bash
  # 2024年度予算と比較 (デフォルト)
  python analysis/check_project_balance_by_year.py

  # 2023年度予算と比較
  python analysis/check_project_balance_by_year.py 2023
  ```

### `analyze_project_balance.py`

- **目的:**
  `check_..._by_year.py`で見つかった「予算超過」事業について、その原因が「国庫債務負担行為」や「マイナス予算」などに起因するのかを自動で推定・分類します。
- **使い方:**
  ```bash
  python analysis/analyze_project_balance.py
  ```

### `compare_execution_rates.py` & `find_consistent_projects.py`

- **目的:**
  元データにある`執行率`と、支出実態から計算した`執行率`の乖離を分析します。`compare...`は乖離が大きい事業を、`find...`は乖離が小さい（一致する）事業をリストアップします。
- **使い方:**
  ```bash
  python analysis/compare_execution_rates.py
  python analysis/find_consistent_projects.py
  ```

---

## 4. 個別テーマ分析とデータ整形

特定の事業を深掘りしたり、他のツールで可視化するためのデータを整形するスクリプトです。

### `get_business_details.py`

- **目的:**
  指定された単一の`予算事業ID`に紐づく全情報を、階層型JSONとして一括で抽出します。
- **使い方:**
  ```bash
  python analysis/get_business_details.py 7259 -o results/business_7259.json
  ```

### `analyze_related_projects.py`

- **目的:**
  指定された事業ID（または予算最大の事業）の「関連事業」構成を分析し、レポートします。
- **使い方:**
  ```bash
  # 予算最大の事業を分析
  python analysis/analyze_related_projects.py
  ```

### `flatten_json_for_looker.py`

- **目的:**
  `get_business_details.py`で生成した階層型JSONを、Looker StudioなどのBIツールで扱いやすい平坦なCSV形式に変換します。サンキーチャートの作成などに利用します。
- **使い方:**
  ```bash
  python analysis/flatten_json_for_looker.py results/business_7259.json looker_ready_7259.csv
  ```

### `extract_text_data.py` & `generate_wordclouds.py`

- **目的:**
  `extract...`でDBからテキストデータを抽出し、`generate...`でそのテキストデータからワードクラウド画像を生成します。
- **使い方:**
  ```bash
  python analysis/extract_text_data.py
  python analysis/generate_wordclouds.py
  ```

## 分析結果の活用

これらのスクリプトで出力されたCSVやJSONファイルは、さらなる分析の入力データとして使ったり、GoogleスプレッドシートにインポートしてLooker Studioで可視化したりすることができます。

---

## 高度な可視化：サンキーチャートによる資金フローの分析

`get_business_details.py` と `flatten_json_for_looker.py` を使って生成した平坦化CSVデータは、**サンキーチャート（Sankey Diagram）** を使って資金の流れを可視化するのに最適です。

サンキーチャートは、「どこから（Source）」「どこへ（Target）」「どれくらいの量（Value）」のフローがあったかを示すグラフで、事業全体の支出構造を直感的に理解するのに役立ちます。

### Looker Studioでの作成手順

Looker Studioの標準機能にはサンキーチャートはありませんが、「コミュニティ可視化」機能を使うことで簡単に追加できます。

#### 1. データの準備

分析したい事業の平坦化CSVデータを用意します。

1.  `get_business_details.py` で、特定の事業IDのJSONデータを `results/` フォルダに出力します。
    ```bash
    python analysis/get_business_details.py [事業ID] -o results/business_[事業ID]_details.json
    ```
2.  `flatten_json_for_looker.py` で、そのJSONを平坦なCSVに変換します。
    ```bash
    python analysis/flatten_json_for_looker.py results/business_[事業ID]_details.json looker_ready_[事業ID].csv
    ```
3.  生成されたCSVファイル (`results/looker_ready_[事業ID].csv`) を、Googleスプレッドシートにインポートします。

#### 2. Looker Studioでのレポート構築

1.  **データソース接続:**
    Looker Studioで新しいレポートを作成し、準備したGoogleスプレッドシートをデータソースとして接続します。

2.  **サンキーチャートの追加:**
    - ツールバーの右端にある「**コミュニティ コンポーネントとライブラリ**」アイコンをクリックします。
    - 「**さらに表示**」を選択し、コミュニティギャラリーを開きます。
    - 検索窓で `Sankey` と検索し、表示された「**Sankey Diagram**」を追加します。
    - メニューバーの「**グラフを追加**」から、追加した「Sankey Diagram」を選択し、レポートのキャンバスに配置します。

3.  **チャートの設定:**
    グラフを選択した状態で、画面右側の「**設定**」パネルに必要な項目をセットします。
    - **Dimension 1 (Source / 起点):**
      - `事業名` をセットします。
    - **Dimension 2 (Target / 終点):**
      - `支出先名` をセットします。
    - **Metric (Value / 流量):**
      - `金額` をセットします。

4.  **調整と解釈:**
    - これで、事業から各支出先への資金の流れが、金額に応じた帯の太さで可視化されます。
    - 支出先が多すぎる場合は、レポートの**フィルタ機能**を使い、「金額が〇〇円以上の支出先のみ」といった条件で絞り込むと、より重要な流れに焦点を当てることができます。
    - 「**スタイル**」タブから、色やラベルの表示を調整して、レポートの完成度を高めましょう。

