# 特定目的分析スクリプト集

このフォルダには、プロジェクトのデータベース(`rs_database.duckdb`)を利用して、特定のテーマや疑問に答えるための、より高度で応用的な分析スクリプトを格納します。

各スクリプトは、プロジェクトのルートディレクトリから実行することを想定しています。

---

## スクリプト一覧

### `get_business_details.py`

- **目的:**
  指定された単一の`予算事業ID`に紐づく、すべてのテーブルの情報を一括で抽出し、階層化された単一のJSONファイルとして出力します。特定の事業を詳細に調査する際の元データ作成に利用します。

- **使い方:**
  ```bash
  # 予算事業ID: 7259 の全情報を、results/フォルダに business_7259.json として保存
  python analysis/get_business_details.py 7259 -o business_7259.json
  ```

### `analyze_related_projects.py`

- **目的:**
  指定された`予算事業ID`（または予算額が最大の事業）について、「関連事業」の構成を分析し、サマリーと詳細リストを出力します。巨大事業がどのような他の事業と連携しているかを調査するために使用します。

- **使い方:**
  - **【自動】予算額最大の事業を分析する場合:**
    ```bash
    python analysis/analyze_related_projects.py
    ```
  - **【手動】特定の予算事業IDを指定して分析する場合:**
    ```bash
    python analysis/analyze_related_projects.py 7259
    ```
  
  実行すると、コンソールに分析サマリーが表示され、`results/`フォルダに詳細な関連事業リストのCSVファイルが `related_projects_for_ID_XXXX.csv` のような名前で保存されます。

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
