import sys
import yaml
import importlib
from pathlib import Path
import argparse
import datetime

# --- プロジェクトルートを基準にパスを解決 ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path().cwd()

def run_scenario_from_yaml(scenario_file: str, output_base_folder: str):
    """
    指定されたYAML形式のシナリオファイルから分析ステップを読み込み、順番に実行する。
    """
    scenario_path = PROJECT_ROOT / scenario_file
    if not scenario_path.is_file():
        print(f"[エラー] シナリオファイル '{scenario_path}' が見つかりません。")
        sys.exit(1)

    print(f"--- 分析シナリオ '{scenario_path.name}' を開始します ---")
    
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = PROJECT_ROOT / output_base_folder / f"{scenario_path.stem}_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f" -> 成果物は '{output_dir}' に保存されます（各スクリプトの出力は'results/'）。")

    try:
        with scenario_path.open('r', encoding='utf-8') as f:
            scenarios = yaml.safe_load(f)
    except Exception as e:
        print(f"[エラー] シナリオファイル '{scenario_path.name}' の読み込みまたは解析に失敗しました: {e}")
        return

    # --- シナリオの実行 ---
    for i, scenario in enumerate(scenarios, 1):
        print("\n" + "="*80)
        print(f"シナリオ {i}/{len(scenarios)}: {scenario.get('scenario_name', '無名のシナリオ')}")
        print("="*80)
        
        if 'purpose' in scenario:
            print(f"\n[目的]\n{scenario['purpose'].strip()}")
        if 'hypothesis' in scenario:
            print(f"\n[仮説]\n{scenario['hypothesis'].strip()}")
            
        for j, step in enumerate(scenario.get('steps', []), 1):
            print("\n" + "-"*60)
            print(f"  ステップ {j}: {step.get('description', '無名ステップ')}")
            print("-"*60)
            
            try:
                script_name = step['script']
                # analysisフォルダ内のモジュールを動的にインポート
                # (例: analysis.audit_text_consistency)
                module = importlib.import_module(f"analysis.{script_name}")
                
                # スクリプト名と同じ名前の関数を呼び出すと仮定
                # (例: audit_text_consistency.py -> audit_consistency() )
                # この規約により、呼び出す関数が明確になる
                target_function = getattr(module, script_name)
                
                # パラメータを渡して関数を実行
                print("  -> 実行中...")
                target_function(**step.get('params', {}))
                print("  -> ステップ完了。")
                
            except ImportError:
                print(f"[エラー] モジュール 'analysis.{script_name}' が見つかりません。ファイル名を確認してください。")
            except AttributeError:
                 print(f"[エラー] モジュール '{script_name}' 内に関数 '{script_name}' が見つかりません。関数名を確認してください。")
            except Exception as e:
                print(f"[エラー] ステップの実行中に予期せぬエラーが発生しました: {e}")
                print("\n以降のシナリオの実行を中止します。")
                return # エラー発生時は中止

    print("\n" + "="*80)
    print("--- 全ての分析シナリオが完了しました ---")
    print("="*80)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="分析シナリオファイル(YAML)に記述されたステップを自動で実行します。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        'scenario_file', 
        type=str,
        nargs='?',
        default='analysis_scenarios/analysis_scenarios.yaml',
        help="実行するシナリオファイルのパス。\n(デフォルト: analysis_scenarios/analysis_scenarios.yaml)"
    )
    parser.add_argument(
        '-o', '--output_folder', 
        type=str,
        default='analysis_outputs',
        help="分析の実行ログ等を保存するベースフォルダ名。\n(デフォルト: analysis_outputs)"
    )
    args = parser.parse_args()
    
    # analysisフォルダ内のスクリプトをインポート可能にするため、パスを追加
    sys.path.append(str(PROJECT_ROOT))
    
    run_scenario_from_yaml(args.scenario_file, args.output_folder)