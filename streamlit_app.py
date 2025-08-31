import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path
import json

# --- 基本設定とパス解決 ---
# Streamlitはスクリプトの場所を基準に動作するため、パス解決がシンプル
PROJECT_ROOT = Path(__file__).parent
SETTINGS_FILE = PROJECT_ROOT / 'project_settings.json'

# --- キャッシュ設定 ---
# 設定ファイルは一度読み込んだらキャッシュする
@st.cache_data
def load_settings():
    """設定ファイルを読み込む"""
    try:
        with SETTINGS_FILE.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"設定ファイル '{SETTINGS_FILE}' の読み込みに失敗しました: {e}")
        return None

# DB接続はリソースとしてキャッシュし、再接続を防ぐ
@st.cache_resource
def get_db_connection(db_path):
    """DuckDBへの接続を確立する"""
    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
        return con
    except Exception as e:
        st.error(f"データベース '{db_path}' への接続に失敗しました: {e}")
        return None

# --- Streamlit アプリケーション本体 ---

# ページの基本設定
st.set_page_config(page_title="RS System SQL Executor", layout="wide")

st.title("📊 RS System - 対話型SQL実行ツール")
st.markdown("""
このツールを使って、`rs_database.duckdb`に対して直接SQLクエリを実行し、結果をインタラクティブに確認できます。
左側のセレクトボックスからサンプルクエリを選ぶか、下のテキストエリアに自由にクエリを記述してください。
""")

# --- 設定とDB接続の準備 ---
settings = load_settings()
if settings:
    db_file_path = PROJECT_ROOT / settings['database']['output_db_file']
    sql_dir_path = PROJECT_ROOT / settings['query_runner'].get('query_directory', 'sql')
    con = get_db_connection(db_file_path)
else:
    st.stop() # 設定が読み込めなければここで停止

if not db_file_path.is_file():
    st.error(f"データベースファイル '{db_file_path}' が見つかりません。")
    st.info("まず、プロジェクトのルートで `python import_zips_to_duckdb.py` を実行して、データベースを構築してください。")
    st.stop()

# --- UIコンポーネントの配置 ---

# 1. サイドバー：サンプルクエリの選択
st.sidebar.header("サンプルクエリ")
try:
    sql_files = sorted([f for f in sql_dir_path.glob('*.sql')])
    sql_file_names = ["<カスタムクエリを入力>"] + [f.name for f in sql_files]
    selected_query_name = st.sidebar.selectbox("クエリを選択:", sql_file_names)
    
    query_text = ""
    if selected_query_name != "<カスタムクエリを入力>":
        query_text = (sql_dir_path / selected_query_name).read_text(encoding='utf-8')
except Exception as e:
    st.sidebar.error(f"SQLファイルの読み込みに失敗しました: {e}")


# 2. メイン画面：クエリエディタと実行ボタン
st.subheader("SQLクエリエディタ")
query_input = st.text_area("ここにSQLクエリを入力してください", value=query_text, height=300)

if st.button("クエリを実行", type="primary"):
    if not query_input:
        st.warning("クエリが入力されていません。")
    else:
        with st.spinner("クエリを実行中..."):
            try:
                result_df = con.execute(query_input).fetchdf()
                
                st.success(f"クエリが完了し、{len(result_df)}件の結果を取得しました。")
                st.subheader("実行結果")
                
                # 結果をインタラクティブなテーブルとして表示
                st.dataframe(result_df)
                
                # CSVダウンロードボタンも追加
                @st.cache_data
                def convert_df_to_csv(df):
                    return df.to_csv(index=False).encode('utf-8-sig')

                csv = convert_df_to_csv(result_df)
                st.download_button(
                    label="結果をCSVでダウンロード",
                    data=csv,
                    file_name='query_result.csv',
                    mime='text/csv',
                )

            except Exception as e:
                st.error("クエリの実行中にエラーが発生しました。")
                # エラーメッセージを整形して表示
                st.code(f"{e}", language="bash")