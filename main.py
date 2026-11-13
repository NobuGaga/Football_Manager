#!/usr/bin/env python3
"""足球经理的推断 · 多联赛概率分析。

常用流程：
  python3 main.py init
  python3 main.py import-fixtures fixtures.csv --date 2026-10-25
  python3 main.py import-features features.csv
  python3 main.py predict --date 2026-10-25
  python3 main.py report --date 2026-10-25
  python3 main.py import-results results.csv
  python3 main.py backtest
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from backtest import Backtester
from database import Database
from importers import AliasImporter, FeatureImporter, FixtureImporter, ResultImporter
from miniprogram_export import export_miniprogram_report
from predictor import FootballPredictor
from reporter import ReportGenerator
from result_checker import ResultChecker
from scraper import SportteryScraper


def init_db(db_path: str = "data/football.db") -> None:
    Database(db_path).initialize()
    print(f"数据库初始化及旧数据迁移完成：{db_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default="data/football.db", help="SQLite 数据库路径")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init", help="初始化数据库并迁移旧 matches 数据")

    fixtures = sub.add_parser("import-fixtures", help="导入手动保存的官方对阵 CSV/JSON/Excel")
    fixtures.add_argument("file")
    fixtures.add_argument("--date", help="时间缺少日期时使用的比赛日期 YYYY-MM-DD")
    fixtures.add_argument("--timezone", default="Asia/Shanghai")
    fixtures.add_argument("--source", default="sporttery_manual")

    scrape = sub.add_parser("scrape", help="读取体彩官方公开接口；失败时改用手动导入")
    scrape.add_argument("--date")

    aliases = sub.add_parser("import-aliases", help="导入球队名称和外部 ID 映射")
    aliases.add_argument("file")

    features = sub.add_parser("import-features", help="导入带 as_of 的赛前特征")
    features.add_argument("file")
    features.add_argument("--source", default="manual_features")

    predict = sub.add_parser("predict", help="按项目 AI 策略文档生成版本化概率和比分推断")
    predict.add_argument("--date")
    predict.add_argument("--refresh", action="store_true", help="临场更新：追加新推断，保留初版")
    predict.add_argument("--as-of", help="历史模拟的推断时间 ISO 8601；不得晚于开赛时间")
    predict.add_argument("--ai-model", default=os.environ.get("AI_MODEL", "kimi"), help="AI 策略分类，默认 kimi")
    sub.add_parser("ai-strategies", help="列出项目内可用的 AI 预测策略分类")

    results = sub.add_parser("import-results", help="从本地赛果文件导入实际比分并关联对阵")
    results.add_argument("file")
    results.add_argument("--source", default="manual_results")
    check = sub.add_parser("check", help="核对 AI 与用户选择，并重新输出预测核对表")
    check.add_argument("--date", help="只输出指定营业日的核对表")
    check.add_argument("--output-dir", default="reports")

    report = sub.add_parser("report", help="指定日期生成每日报告；不指定则生成回测 Excel")
    report.add_argument("--date")
    report.add_argument("--output-dir", default="reports")

    export = sub.add_parser("export-kimi", help="生成供 Kimi 读取的每日 JSON/CSV/Excel/Markdown")
    export.add_argument("--date", required=True)
    export.add_argument("--output-dir", default="reports")

    checklist = sub.add_parser("export-checklist", help="输出含原始赔率和用户选择的预测核对表")
    checklist.add_argument("--date", required=True)
    checklist.add_argument("--output-dir", default="reports")
    checklist.add_argument("--output")
    checklist.add_argument("--ai-model", default=os.environ.get("AI_MODEL", "kimi"))

    strategy = sub.add_parser("analyze-strategy", help="生成双选比分、正路与偏态方向策略")
    strategy.add_argument("--date", required=True)
    strategy.add_argument("--output-dir", default="reports")
    strategy.add_argument("--ai-model", default=os.environ.get("AI_MODEL", "kimi"))

    checklist_import = sub.add_parser("import-checklist", help="导入实际下注与赛果，回写核对表和历史数据")
    checklist_import.add_argument("file")
    checklist_import.add_argument("--output-dir", default="reports")

    checklist_validation = sub.add_parser("validate-checklist", help="独立重算并校验预测核对表全部结论")
    checklist_validation.add_argument("file")
    checklist_validation.add_argument("--output")

    history_import = sub.add_parser("import-history", help="迁移旧版足球预测历史数据，不伪造概率")
    history_import.add_argument("file")
    history_import.add_argument("--year", type=int, help="MM-DD 比赛时间使用的年份；有完整年份时可省略")
    history_import.add_argument("--output-dir", default="reports")

    history_export = sub.add_parser("export-history", help="从数据库去重并导出足球预测历史数据 CSV")
    history_export.add_argument("--output", default="reports/足球预测历史数据.csv")

    strategy_validation = sub.add_parser("validate-strategy", help="校验策略声明、历史命中与偏态候选")
    strategy_validation.add_argument("--date")
    strategy_validation.add_argument("--output-dir", default="reports")
    strategy_validation.add_argument("--update-policy", action="store_true", help="将历史样本校准元数据写回策略 MD")

    mini_export = sub.add_parser("export-miniprogram", help="将每日 JSON 报告打包为小程序数据")
    mini_export.add_argument("--date", required=True)
    mini_export.add_argument("--report", help="每日 JSON 路径，默认读取 reports/ 下对应日期报告")
    mini_export.add_argument("--output", default="web/generated/daily-report.js")

    backtest = sub.add_parser("backtest", help="计算 Log Loss、Brier、校准和比分指标")
    backtest.add_argument("--model-version")
    backtest.add_argument("--league")
    backtest.add_argument("--output")

    sub.add_parser("review-queue", help="列出历史遗留的待确认名称或比赛匹配")

    all_cmd = sub.add_parser("all", help="对阵导入/抓取、策略推断和每日报告")
    all_cmd.add_argument("--date", required=True)
    all_cmd.add_argument("--fixtures", help="官方对阵文件；不提供时读取体彩官方公开接口")
    all_cmd.add_argument("--timezone", default="Asia/Shanghai")
    all_cmd.add_argument("--output-dir", default="reports")
    all_cmd.add_argument("--ai-model", default=os.environ.get("AI_MODEL", "kimi"))

    daily = sub.add_parser("daily", help="每日赛果、推断、双选策略、核对表、历史和本地产物安全检查全流程")
    daily.add_argument("--date", help="体彩营业日，默认 Asia/Shanghai 今天")
    daily.add_argument("--results-date", help="赛果日期，默认对阵日期前一天")
    daily.add_argument("--fixtures", help="官方对阵文件；不提供时读取体彩官方公开接口")
    daily.add_argument("--results", help="昨日赛果文件")
    daily.add_argument("--output-dir", default="reports")
    daily.add_argument("--ai-model", default=os.environ.get("AI_MODEL", "kimi"))
    daily.add_argument("--refresh", action="store_true", help="追加临场推断版本")
    daily.add_argument("--require-results", action="store_true", help="昨日有比赛但未可靠回填时中止")
    daily.add_argument("--pull", action="store_true", help="开始前 git pull --rebase")
    daily.add_argument("--push", action="store_true", help="旧命令兼容参数；不会提交或推送本地关键文件")
    return parser


def print_result(value) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    db = args.db
    if args.command == "init":
        init_db(db)
    elif args.command == "import-fixtures":
        print_result(FixtureImporter(db).import_file(args.file, args.date, args.source, args.timezone))
    elif args.command == "scrape":
        frame = SportteryScraper(db).fetch_daily_matches(args.date)
        print(f"抓取并保存 {len(frame)} 场比赛")
    elif args.command == "import-aliases":
        print_result(AliasImporter(db).import_file(args.file))
    elif args.command == "import-features":
        print_result(FeatureImporter(db).import_file(args.file, args.source))
    elif args.command == "ai-strategies":
        from ai_strategies import available_ai_strategies
        print_result(available_ai_strategies())
    elif args.command == "predict":
        predictions = FootballPredictor(db, ai_model=args.ai_model).predict_all(args.date, args.refresh, args.as_of)
        print(f"新增 {len(predictions)} 条推断；既有推断未被覆盖")
    elif args.command == "import-results":
        print_result(ResultImporter(db).import_file(args.file, args.source))
    elif args.command == "check":
        from checklist import ChecklistGenerator

        checked = len(ResultChecker(db).check_all())
        if args.date:
            dates = [args.date]
        else:
            with Database(db).connect() as conn:
                dates = [row["business_date"] for row in conn.execute("""SELECT DISTINCT f.business_date
                    FROM predictions p JOIN fixtures f ON f.fixture_key=p.fixture_key
                    JOIN results r ON r.fixture_key=p.fixture_key ORDER BY f.business_date""")]
        generator = ChecklistGenerator(db, args.output_dir)
        paths = [generator.generate(date)["csv"] for date in dates]
        print_result({"predictions_checked": checked, "checklists": paths})
    elif args.command in {"report", "export-kimi"}:
        reporter = ReportGenerator(db, args.output_dir)
        if args.command == "export-kimi" or args.date:
            print_result(reporter.generate_daily_report(args.date))
        else:
            print(f"回测报表：{reporter.generate_all_reports()}")
    elif args.command == "export-checklist":
        from checklist import ChecklistGenerator
        print_result(ChecklistGenerator(db, args.output_dir, args.ai_model).generate(args.date, args.output))
    elif args.command == "analyze-strategy":
        from strategy_engine import StrategyEngine
        print_result(StrategyEngine(db, args.output_dir, ai_model=args.ai_model).generate(args.date))
    elif args.command == "import-checklist":
        from checklist import ChecklistImporter
        print_result(ChecklistImporter(db).import_file(args.file, args.output_dir))
    elif args.command == "validate-checklist":
        from checklist import ChecklistValidator
        validator = ChecklistValidator()
        result = validator.write_report(args.file, args.output) if args.output else validator.validate_file(args.file)
        print_result(result)
        if not result["valid"]:
            raise ValueError(f"预测核对表校验失败：{result['error_count']} 个错误")
    elif args.command == "import-history":
        from datetime import datetime
        from zoneinfo import ZoneInfo
        from history import HistoryManager
        manager = HistoryManager(db)
        result = manager.import_file(args.file, args.year)
        result["export"] = manager.export(Path(args.output_dir) / "足球预测历史数据.csv")
        report_date = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
        validation_path = Path(args.output_dir) / f"历史数据导入校验_{report_date}.json"
        validation_path.parent.mkdir(parents=True, exist_ok=True)
        result["validation_report"] = str(validation_path)
        validation_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print_result(result)
    elif args.command == "export-history":
        from history import HistoryManager
        print_result(HistoryManager(db).export(args.output))
    elif args.command == "validate-strategy":
        from strategy_engine import StrategyValidator
        print_result(StrategyValidator(db, args.output_dir).generate(args.date, args.update_policy))
    elif args.command == "export-miniprogram":
        repo_root = Path(__file__).resolve().parent
        report_path = Path(args.report) if args.report else repo_root / "reports" / f"足球经理的推断_{args.date}.json"
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = repo_root / output_path
        print(export_miniprogram_report(report_path, output_path))
    elif args.command == "backtest":
        result = Backtester(db).evaluate(args.model_version, args.league)
        if args.output:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"回测结果：{output}")
        else:
            print_result(result)
    elif args.command == "review-queue":
        with Database(db).connect() as conn:
            rows = [dict(row) for row in conn.execute("SELECT * FROM match_review_queue WHERE status='open' ORDER BY id")]
        print_result(rows)
    elif args.command == "all":
        if args.fixtures:
            print_result(FixtureImporter(db).import_file(args.fixtures, args.date, timezone_name=args.timezone))
        else:
            SportteryScraper(db).fetch_daily_matches(args.date)
        predictions = FootballPredictor(db, ai_model=args.ai_model).predict_all(args.date)
        print(f"新增 {len(predictions)} 条推断")
        print_result(ReportGenerator(db, args.output_dir).generate_daily_report(args.date))
    elif args.command == "daily":
        from automation import DailyAutomation
        repo_root = Path(__file__).resolve().parent
        db_path = Path(db)
        output_dir = Path(args.output_dir)
        if not db_path.is_absolute():
            db_path = repo_root / db_path
        if not output_dir.is_absolute():
            output_dir = repo_root / output_dir
        status = DailyAutomation(repo_root, db_path, output_dir).run(
            date=args.date, results_date=args.results_date, fixtures=args.fixtures, results=args.results,
            refresh=args.refresh, require_results=args.require_results, pull=args.pull, push=args.push,
            ai_model=args.ai_model,
        )
        print(Path(status["summary_file"]).read_text(encoding="utf-8") if Path(status["summary_file"]).is_absolute()
              else (repo_root / status["summary_file"]).read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, FileNotFoundError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(2)
