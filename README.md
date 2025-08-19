# RSシステムCSVデータをDuckDBにする

[RSシステムCSVデータ](https://rssystem.go.jp/download-csv)からダウンロードしたZIP形式のCSVデータを抽出し、テーブルごとに独立したDuckDB形式のデータベースファイルへ変換・管理するためのプロジェクトです。

## 概要

このプロジェクトは、`download/` フォルダに格納された行政事業レビューシートのZIPファイル群を読み込み、個別のDuckDBデータベースファイルに変換します。生成されたデータベースファイルは、GitHub Releasesを通じて配布され、WEBアプリケーションなどから参照されることを想定しています。

また、データベースの構造（スキーマ）を記述した`schema.json`および`schema.yaml`を自動生成し、データの可読性と再利用性を高めます。

## 特徴

- **データ分離:** ソースコードとデータファイルを分離し、Gitリポジトリを軽量に保ちます。
- **再現性:** `import_zips_to_duckdb.py` を実行するだけで、誰でも同じデータベースファイルを生成できます。
- **ポータビリティ:** データはテーブルごとに独立したDuckDBファイルに分割され、管理が容易です。
- **ドキュメント化:** データベースのスキーマ情報はJSONとYAML形式でエクスポートされ、ドキュメントとして機能します。

## 使い方

### 1. セットアップ

**必要なもの:**
- Python 3.8以上

**手順:**
1.  このリポジトリをクローンします。
    ```bash
    git clone https://github.com/<YOUR_GITHUB_USERNAME>/<YOUR_REPOSITORY_NAME>.git
    cd <YOUR_REPOSITORY_NAME>
    ```

2.  必要なPythonライブラリをインストールします。
    ```bash
    pip install -r requirements.txt
    ```
    (`requirements.txt`がない場合は、以下のコマンドでインストールしてください)
    ```bash
    pip install pandas duckdb pyyaml requests
    ```

3.  `download/` フォルダを作成し、行政事業レビューシートのZIPファイルを格納します。

### 2. データベースの生成

ローカル環境でデータベースファイルを生成するには、以下のコマンドを実行します。
`duckdb_files/` フォルダが作成され、その中にテーブルごとの `.duckdb` ファイルが生成されます。

```bash
python import_zips_to_duckdb.py
```

### 3. データベースの検証

生成されたデータベースファイルが正しいか検証します。

```bash
python verify_database.py```

### 4. スキーマ情報のエクスポート

データベースの構造を `schema.json` と `schema.yaml` に出力します。

```bash
python export_schemas.py
```

## データセットの配布と利用 (GitHub Releases)

このプロジェクトでは、生成されたデータベースファイルをGitリポジトリに直接含めず、**GitHub Releases** を利用して配布します。

### データセットの発行 (開発者向け)

1.  上記の手順で `duckdb_files/` フォルダを生成します。
2.  `duckdb_files/` フォルダを一つのZIPファイル（例: `database_v1.0.zip`）に圧縮します。
3.  本リポジトリの **Releases** ページで新しいリリースを作成し、圧縮したZIPファイルをアセットとしてアップロードします。

### データセットの利用 (アプリケーション向け)

WEBアプリケーションなどからデータベースを利用する場合、`setup_database.py` を使用して自動でデータをダウンロード・展開できます。

`setup_database.py` 内の以下の項目を、対象のリリースタグに合わせて編集してください。
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `RELEASE_TAG`
- `ASSET_FILENAME`

その後、以下のコマンドを実行すると `duckdb_files/` フォルダがセットアップされます。

```bash
python setup_database.py
```

## ライセンス

このプロジェクトは [MIT License](LICENSE) の下で公開されています。