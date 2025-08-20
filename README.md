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
- **強力な分析ツール:** 付属のSQL実行スクリプト (`run_query.py`) を使って、コマンドラインから柔軟なデータ抽出・分析が可能です。

## プロジェクト構造

```
.
├── .gitignore              # Gitの管理対象外ファイルを指定
├── default_query.sql       # デフォルトで実行されるサンプルSQL
├── export_schemas.py       # スキーマ出力スクリプト
├── import_zips_to_duckdb.py # DB生成スクリプト
├── LICENSE                 # ライセンスファイル
├── project_settings.json   # プロジェクトの全設定を管理するファイル
├── README.md               # このファイル
├── requirements.txt        # 必要なPythonライブラリ
├── run_query.py            # SQL実行スクリプト
├── schema.json             # 生成されたスキーマ定義
├── schema.yaml             # 生成されたスキーマ定義
└── verify_database.py      # DB検証スクリプト
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

### 2. データベースの生成

ローカル環境で単一のデータベースファイルを生成します。`project_settings.json`で指定されたパスに `.duckdb` ファイルが生成されます。

```bash
python import_zips_to_duckdb.py
```

### 3. データベースの検証とスキーマ出力

- **検証:** 生成されたDBファイル内のテーブル、VIEW、インデックスが正しいか検証します。
  ```bash
  python verify_database.py
  ```
- **スキーマ出力:** DBの構造を `schema.json` と `schema.yaml` に出力します。
  ```bash
  python export_schemas.py
  ```

### 4. データベースへのクエリ実行

`run_query.py` を使って、データベースにSQLクエリを実行します。クエリは**日本語のVIEW名**を使って書くことを推奨します。

- **デフォルトクエリを実行 (結果はCSVに出力):**
  ```bash
  python run_query.py
  ```
  `project_settings.json` で指定されたデフォルトクエリが実行され、結果が `results/` フォルダに保存されます。

- **指定したSQLファイルを実行し、結果をファイルに出力:**
  ```bash
  python run_query.py -q sample_query.sql -o my_analysis.csv
  ```

- **結果をファイルに出力せず、コンソールで確認するだけ:**
  ```bash
  python run_query.py --no-output
  ```

## データセットの配布と利用 (GitHub Releases)

このプロジェクトでは、生成されたデータベースファイルを **GitHub Releases** を利用して配布します。

### データセットの発行 (開発者向け)

1.  上記の手順で単一のデータベースファイル（例: `rs_database.duckdb`）を生成します。
2.  このDBファイルを一つのZIPファイル（例: `database_v1.0.zip`）に圧縮します。
3.  本リポジトリの **Releases** ページで新しいリリースを作成し、圧縮したZIPファイルをアセットとしてアップロードします。

### データセットの利用 (アプリケーション向け)

WEBアプリケーション等でデータベースを利用する場合、`setup_database.py` を使って自動でデータをダウンロード・展開できます。

`setup_database.py` 内の以下の項目を、対象のリリースタグに合わせて編集してください。
- `GITHUB_OWNER`, `GITHUB_REPO`, `RELEASE_TAG`, `ASSET_FILENAME`

その後、以下のコマンドを実行するとDBファイルがセットアップされます。

```bash
python setup_database.py
```

## ライセンス

このプロジェクトは [MIT License](LICENSE) の下で公開されています。