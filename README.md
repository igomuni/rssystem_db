# RSシステムCSVデータをDuckDBにする

[RSシステムCSVデータ](https://rssystem.go.jp/download-csv)からダウンロードしたZIP形式のCSVデータを抽出し、**単一のDuckDBデータベースファイル**へ変換・管理するためのプロジェクトです。

## 概要

このプロジェクトは、設定ファイル(`project_settings.json`)に基づき、`download/`フォルダに格納された行政事業レビューシートのZIPファイル群を処理します。すべてのデータを一つのDuckDBファイルに集約し、その際に以下の処理を自動で行います。

1.  **テーブルの作成:** `tbl_1_1` のような機械的に扱いやすい名前でテーブルを作成します。
2.  **VIEWの作成:** `基本情報_組織情報` のような人間が読んで分かりやすい名前の**VIEW（仮想テーブル）**を作成し、直感的なデータアクセスを可能にします。
3.  **インデックスの作成:** テーブル名、VIEW名、元のファイル名をマッピングした`table_index`テーブルを作成し、データベースの自己説明性を高めます。

生成されたデータベースファイルは、GitHub Releasesを通じて配布され、WEBアプリケーションでの利用やデータ分析の現場で活用されることを想定しています。

## 特徴

- **All-in-Oneデータベース:** 全てのデータが単一のDuckDBファイルに集約され、ポータビリティに優れています。
- **直感的なクエリ:** `SELECT * FROM "基本情報_組織情報"` のように、日本語の分かりやすいVIEW名を使ってSQLクエリを記述できます。
- **設定ファイル駆動:** `project_settings.json` で入出力パスやデフォルトの挙動を管理するため、コードを変更することなく環境に適応できます。
- **再現性とデータ分離:** ソースコードとデータファイルを分離し、誰でも同じデータベースを再現できるクリーンな構成です。
- **強力な分析ツール:** 付属のSQL実行スクリプト (`run_query.py`) や、特定の分析スクリプト (`analysis/`内) を使って、柔軟なデータ抽出・分析が可能です。

## プロジェクト構造

```
.
├── .gitignore
├── LICENSE
├── README.md
├── requirements.txt
|
├── project_settings.json   # ★ プロジェクト全体の「設計図」
|
├── import_zips_to_duckdb.py # DBを「作る」スクリプト (コア)
├── verify_database.py      # DBを「確かめる」スクリプト (コア)
├── export_schemas.py       # DBの「構造を書き出す」スクリプト (コア)
├── run_query.py            # DBに汎用的な「質問をする」ツール (コア)
├── default_query.sql       # デフォルトSQLクエリ(参考用)
|
├── sql/                    # 汎用的な「質問文（SQL）」の置き場所
│   ├── README.md
│   └── ...
|
└── analysis/               # ★ 特定の「分析や応用」を行うスクリプトの置き場所
    └── get_business_details.py
```

## 使い方

### 1. セットアップ

**手順:**
1.  このリポジトリをクローンし、ディレクトリに移動します。
2.  `project_settings.json` を開き、必要に応じてフォルダ名などを環境に合わせて変更します。
3.  必要なPythonライブラリをインストールします。
    ```bash
    pip install -r requirements.txt
    ```
4.  `project_settings.json` で指定された入力フォルダ（デフォルト: `download/`）を作成し、行政事業レビューシートのZIPファイルを格納します。

### 2. データベースの生成と検証

- **DB生成:** ローカル環境で単一のデータベースファイルを生成します。
  ```bash
  python import_zips_to_duckdb.py
  ```
- **検証:** 生成されたDBファイル内のテーブル、VIEW、インデックスが正しいか検証します。
  ```bash
  python verify_database.py
  ```
- **スキーマ出力:** DBの構造を `schema.json` と `schema.yaml` に出力します。
  ```bash
  python export_schemas.py
  ```

### 3. データ分析の実行

#### A) 汎用的なSQLクエリの実行

`run_query.py` を使って、`sql/` フォルダ内のSQLクエリなどを実行します。

- **デフォルトクエリを実行 (結果は`results/`にCSVで保存):**
  ```bash
  python run_query.py
  ```
- **指定したSQLファイルを実行し、結果をファイルに出力:**
  ```bash
  python run_query.py -q sql/my_analysis.sql -o my_result.csv
  ```

#### B) 特定の分析スクリプトの実行

`analysis/` フォルダ内のスクリプトを使い、より高度な分析を行います。

- **特定の予算事業IDに紐づく全情報をJSONで出力:**
  ```bash
  # ID:7259の情報を、results/business_7259.json に保存
  python analysis/get_business_details.py 7259 -o business_7259.json
  ```

## データセットの配布と利用 (GitHub Releases)

このプロジェクトでは、生成されたデータベースファイルを **GitHub Releases** を利用して配布します。（手順の詳細は[こちら](https://docs.github.com/ja/repositories/releasing-projects-on-github/managing-releases-in-a-repository)を参照）

### データセットの発行 (開発者向け)

1.  上記の手順で単一のデータベースファイル（例: `rs_database.duckdb`）を生成します。
2.  このDBファイルを一つのZIPファイル（例: `database_v1.0.zip`）に圧縮します。
3.  本リポジトリの **Releases** ページで新しいリリースを作成し、圧縮したZIPファイルをアセットとしてアップロードします。

## ライセンス

このプロジェクトは [MIT License](LICENSE) の下で公開されています。