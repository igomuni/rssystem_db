# 特定目的分析スクリプト集

このフォルダには、プロジェクトのデータベース(`rs_database.duckdb`)を利用して、特定のテーマや疑問に答えるための、より高度で応用的な分析スクリプトを格納します。

各スクリプトは、プロジェクトのルートディレクトリから実行することを想定しています。

---

## スクリプト一覧と分析シナリオ

### 1. 個別事業の詳細調査

#### `get_business_details.py`

- **目的:**
  指定された単一の`予算事業ID`に紐づく、すべてのテーブルの情報を一括で抽出し、階層化された単一のJSONファイルとして出力します。特定の事業を詳細に調査する際の元データ作成に利用します。

- **使い方:**
  ```bash
  # 予算事業ID: 7259 の全情報を、results/フォルダに business_7259.json として保存
  python analysis/get_business_details.py 7259 -o business_7259.json
  ```

---

### 2. データ品質の「健康診断」

#### `generate_data_quality_report.py`

- **目的:**
  データベース全体をスキャンし、「支出先名が空欄」「予算額が0以下」といった、データ品質に関する潜在的な問題を網羅的に洗い出します。分析を始める前のデータクレンジングや、データの健全性を確認するために使用します。ルールはスクリプト内で定義されており、拡張も可能です。

- **使い方:**
  ```bash
  python analysis/generate_data_quality_report.py
  ```
  実行すると、コンソールに問題のサマリーが表示され、`results/`フォルダに詳細な問題リスト (`data_quality_long_list.csv`) が保存されます。

---

### 3. 予算と支出のバランス分析（一連の調査）

国の予算が適切に執行されているかを検証するための一連のスクリプトです。この調査を通じて、このデータセットの会計上の重要な特性が明らかになりました。

#### Step 1: `calculate_execution_rates.py` (基礎データの作成)

- **目的:** 全事業の**予算執行率**を計算し、府省庁情報を付与した分析の基礎となるサマリーファイルを作成します。
- **使い方:** `python analysis/calculate_execution_rates.py`

#### Step 2: `check_project_balance.py` (矛盾の発見)

- **目的:** 事業単位で「支出総額」が「予算総額」を上回っている、**「予算超過」**が疑われる事業をリストアップします。
- **使い方:** `python analysis/check_project_balance.py`

#### Step 3: `analyze_project_balance.py` (原因の推定)

- **目的:** Step 2で発見された「予算超過」事業について、その原因が**「国庫債務負担行為」**や**「マイナス予算」**などに起因するのかを自動で推定・分類し、詳細なレポートを出力します。
- **使い方:** `python analysis/analyze_project_balance.py`

#### Step 4: `verify_kokko_saimu_hypothesis.py` (仮説の最終検証)

- **目的:** 「予算超過の原因は国庫債務負担行為である」という仮説を逆検証します。このスクリプトにより、このデータセットでは**ほぼ全ての事業が国庫債務負担行為として登録されている**という重要な法則が発見されました。
- **使い方:** `python analysis/verify_kokko_saimu_hypothesis.py`

#### Step 5: `compare_execution_rates.py` & `find_consistent_projects.py` (最終結論)

- **目的:**
  - `compare...`: 元データにある`執行率`と、支出情報から計算した`実態ベースの執行率`の**乖離が大きい**事業をリストアップします。
  - `find...`: 逆に、2つの執行率が**ほぼ一致**する事業をリストアップします。
- **結論:** この比較により、「予算超過」はデータ入力ミスではなく、**単年度の予算管理情報と、複数年度契約を含む支出実態情報という、異なる会計レイヤーのデータを比較したことによる「見かけ上の矛盾」である**ことが結論付けられました。
- **使い方:**
  ```bash
  python analysis/compare_execution_rates.py
  python analysis/find_consistent_projects.py
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

