# 特定目的分析スクリプト集

このフォルダには、プロジェクトのデータベース(`rs_database.duckdb`)を利用して、特定のテーマや疑問に答えるための、より高度で応用的な分析スクリプトを格納します。

各スクリプトは、プロジェクトのルートディレクトリから実行することを想定しています。

---

## 1. 自然言語での対話型分析 (RAG System)

このプロジェクトの最終目標である、自然言語でデータベースに質問するためのコア機能です。**RAG (Retrieval-Augmented Generation)** というAIアーキテクチャを採用しています。

### ワークフロー

1.  **DuckDBの構築:** ルートの`import_zips_to_duckdb.py`で`rs_database.db`を作成。
2.  **ベクトルDBの構築:** `build_vector_store.py`でテキストデータの「意味」をベクトル化。
3.  **自然言語で質問:** `ask_with_rag.py`で、日本語の質問からSQLを自動生成・実行。

### `ask_with_rag.py`

- **目的:** 自然言語の質問をAIが解釈し、ベクトル検索とLLMを駆使して高精度なSQLを自動生成・実行します。
- **使い方:**
  ```bash
  python analysis/ask_with_rag.py "ガソリン減税に関連しそうな事業の支出先トップ3は？"
  ```

### `build_vector_store.py`

- **目的:** `ask_with_rag.py`が使用するベクトルデータベースを構築します。
- **使い方 (初回またはDB更新時に実行):**
  ```bash
  # (.envファイルにAPIキーの設定が必要です)
  python analysis/build_vector_store.py
  ```

---

## 2. 分析シナリオの自動実行エンジン

### `run_scenario.py`

- **目的:**
  この分析プラットフォームの**自動実行エンジン**です。後述するYAML形式の「分析計画書」を読み込み、そこに記述された一連の分析ステップを**自動的に、順番に実行**します。

- **使い方:**
  ```bash
  # デフォルトのシナリオファイル(analysis_scenarios/analysis_scenarios.yaml)を実行
  python analysis/run_scenario.py

  # 別のシナリオファイルを指定して実行
  python analysis/run_scenario.py path/to/another_scenario.yaml
  ```
  これにより、再現可能な分析ワークフローを、コマンド一つで実行できます。

### 分析シナリオYAMLファイルの書き方

分析の目的、仮説、そして実行する一連のステップを、`analysis_scenarios/`フォルダ内の`.yaml`ファイルに記述します。

#### 基本構造

YAMLファイルは、「シナリオ」のリストとして構成されます。各シナリオは、以下のキーを持ちます。

- `scenario_name`: (必須) シナリオ全体の名前。
- `purpose`: (推奨) この分析シナリオの目的を記述します。
- `hypothesis`: (推奨) この分析を通じて検証したい仮説を記述します。
- `steps`: (必須) 実行する「ステップ」のリスト。

各ステップは、以下のキーを持ちます。

- `description`: (必須) このステップが何をするかの説明。
- `script`: (必須) 呼び出す`analysis/`フォルダ内のPythonスクリプト名（`.py`は不要）。
- `params`: (任意) スクリプトに渡すコマンドライン引数を、キーと値のペアで記述します。

#### サンプル (`analysis_scenarios/analysis_scenarios.yaml`)
```yaml
- scenario_name: "シナリオ1：府省庁ごとのデータ品質比較"
  purpose: |
    各府省庁が登録している事業データにおいて、「事業名」と「契約概要」の
    論理的な整合性に差があるかを統計的に検証する。
  steps:
    - description: "1.1 デジタル庁のデータ監査"
      script: audit_text_consistency
      params:
        sample_size: 100
        ministry: "デジタル庁"
        free_tier_safe: True # ← ★注釈対象

- scenario_name: "シナリオ2：「高額案件」の集中監査"
  # ...
```

#### **`free_tier_safe`フラグについて (重要)**

`audit_text_consistency.py`などのLLMを呼び出すスクリプトでは、`free_tier_safe`というパラメータを指定できます。

- **目的:** Google AI Studioなどの**無料API枠**に設定されている、1分間あたりのリクエスト数上限（レートリミット）に達するのを防ぎます。
- **機能:** このフラグを `True` に設定すると、スクリプトは以下の「安全運転」モードで動作します。
    1.  各APIリクエストの間に**1秒間の待機時間**を設けます。
    2.  レートリミットエラー（429）が発生した場合、**自動的に待機し、処理を再試行**します。
- **使い方:**
  - **無料APIキーを使っている場合:** 安定した実行のために、**必ず `free_tier_safe: True` を設定**してください。
  - **有料プランに移行した場合:** このフラグを**省略または `False` に設定**することで、待機時間なく、最速で分析を実行できます。

---

## 3. AIによるデータ監査と品質チェック

LLMを活用して、データの品質や整合性を「意味」のレベルで監査するスクリプト群です。

### `audit_text_consistency.py`

- **目的:** LLMに「会計検査官」の役割を与え、「事業名」と「契約概要」の論理的な整合性を自動でスコアリング・評価します。`run_scenario.py`から呼び出されることを主目的とします。

### `generate_data_quality_report.py`

- **目的:** データベース全体をスキャンし、「明細行なのに支出先名が空欄」など、データの構造を理解した上で、品質が低い可能性のあるレコードを網羅的にリストアップします。
- **使い方:**
  ```bash
  python analysis/generate_data_quality_report.py
  ```

---

## 4. 予算と支出の構造分析

事業ごとの予算と支出の関係性を多角的に分析し、データセットの会計上の特性を解き明かすための一連のスクリプトです。

- **`check_project_balance_by_year.py`**: 指定年度の予算と支出総額を比較し、「見かけ上の予算超過」事業をリストアップします。
- **`analyze_project_balance.py`**: 「予算超過」の原因を「国庫債務負担行為」などに自動で推定・分類します。
- **`compare_execution_rates.py` / `find_consistent_projects.py`**: 元データと実態ベースの執行率の乖離を分析し、会計処理の複雑さを評価します。

---

## 5. 個別テーマ分析とデータ整形

特定の事業を深掘りしたり、他のツールで可視化するためのデータを整形するスクリプトです。

- **`get_business_details.py`**: 特定の事業IDの全情報をJSONで一括抽出します。
- **`analyze_related_projects.py`**: 特定事業の「関連事業」構成を分析します。
- **`flatten_json_for_looker.py`**: 階層型JSONを、Looker Studio用の平坦なCSVに変換します。
- **`extract_text_data.py`**: DBから分析用のテキストデータをCSVとして抽出します。
- **`validate_summary_details_split.py`**: 分割したサマリー/明細テーブルの金額の整合性を検証します。
- **`validate_details_breakdown.py`**: 支出明細とその費目・使途の内訳の乖離を調査します。

---

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

