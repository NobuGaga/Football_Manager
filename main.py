#!/usr/bin/env python3
"""竞彩足球多联赛批量预测分析系统。

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

    scrape = sub.add_parser("scrape", help="尝试读取官方公开网页；失败时改用手动导入")
    scrape.add_argument("--date")

    aliases = sub.add_parser("import-aliases", help="导入球队名称和外部 ID 映射")
    aliases.add_argument("file")

    features = sub.add_parser("import-features", help="导入带 as_of 的赛前特征")
    features.add_argument("file")
    features.add_argument("--source", default="manual_features")

    enrich = sub.add_parser("enrich", help="使用 API-Football 补充赛前特征")
    enrich.add_argument("--date", required=True)

    sync_results = sub.add_parser("sync-results", help="使用 API-Football 回填指定日期的已完结赛果")
    sync_results.add_argument("--date", required=True)

    predict = sub.add_parser("predict", help="生成版本化概率和比分预测")
    predict.add_argument("--date")
    predict.add_argument("--refresh", action="store_true", help="临场更新：追加新预测，保留初版")
    predict.add_argument("--as-of", help="历史模拟的预测时间 ISO 8601；不得晚于开赛时间")

    results = sub.add_parser("import-results", help="导入实际比分并关联对阵")
    results.add_argument("file")
    results.add_argument("--source", default="manual_results")
    sub.add_parser("check", help="核对所有已出赛果的预测")

    report = sub.add_parser("report", help="指定日期生成每日报告；不指定则生成回测 Excel")
    report.add_argument("--date")
    report.add_argument("--output-dir", default="reports")

    export = sub.add_parser("export-kimi", help="生成供 Kimi 读取的每日 JSON/CSV/Excel/Markdown")
    export.add_argument("--date", required=True)
    export.add_argument("--output-dir", default="reports")

    backtest = sub.add_parser("backtest", help="计算 Log Loss、Brier、校准和比分指标")
    backtest.add_argument("--model-version")
    backtest.add_argument("--league")
    backtest.add_argument("--output")

    sub.add_parser("review-queue", help="列出需要人工确认的名称或外部比赛匹配")

    all_cmd = sub.add_parser("all", help="对阵导入/抓取、预测和每日报告")
    all_cmd.add_argument("--date", required=True)
    all_cmd.add_argument("--fixtures", help="官方对阵文件；不提供时尝试网页抓取")
    all_cmd.add_argument("--timezone", default="Asia/Shanghai")
    all_cmd.add_argument("--output-dir", default="reports")

    daily = sub.add_parser("daily", help="Kimi Claw 每日赛果、预测、回测和 Git 推送全流程")
    daily.add_argument("--date", help="对阵日期，默认 Asia/Shanghai 今天")
    daily.add_argument("--results-date", help="赛果日期，默认对阵日期前一天")
    daily.add_argument("--fixtures", help="官方对阵文件；不提供时抓取公开网页")
    daily.add_argument("--results", help="昨日赛果文件；优先于 API 同步")
    daily.add_argument("--output-dir", default="reports")
    daily.add_argument("--enrich", action="store_true", help="使用 API-Football 补充今日赛前特征")
    daily.add_argument("--sync-results", action="store_true", help="使用 API-Football 回填昨日赛果")
    daily.add_argument("--refresh", action="store_true", help="追加临场预测版本")
    daily.add_argument("--require-results", action="store_true", help="昨日有比赛但未可靠回填时中止")
    daily.add_argument("--pull", action="store_true", help="开始前 git pull --rebase")
    daily.add_argument("--push", action="store_true", help="成功后仅提交数据产物并 git push")
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
    elif args.command == "enrich":
        from providers import APIFootballEnricher
        print_result(APIFootballEnricher(db).enrich(args.date))
    elif args.command == "sync-results":
        from providers import APIFootballEnricher
        print_result(APIFootballEnricher(db).sync_results(args.date))
    elif args.command == "predict":
        predictions = FootballPredictor(db).predict_all(args.date, args.refresh, args.as_of)
        print(f"新增 {len(predictions)} 条预测；既有预测未被覆盖")
    elif args.command == "import-results":
        print_result(ResultImporter(db).import_file(args.file, args.source))
    elif args.command == "check":
        print(f"已核对 {len(ResultChecker(db).check_all())} 条版本化预测")
    elif args.command in {"report", "export-kimi"}:
        reporter = ReportGenerator(db, args.output_dir)
        if args.command == "export-kimi" or args.date:
            print_result(reporter.generate_daily_report(args.date))
        else:
            print(f"回测报表：{reporter.generate_all_reports()}")
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
        predictions = FootballPredictor(db).predict_all(args.date)
        print(f"新增 {len(predictions)} 条预测")
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
            enrich=args.enrich, sync_results=args.sync_results, refresh=args.refresh,
            require_results=args.require_results, pull=args.pull, push=args.push,
        )
        print(Path(status["summary_file"]).read_text(encoding="utf-8") if Path(status["summary_file"]).is_absolute()
              else (repo_root / status["summary_file"]).read_text(encoding="utf-8"))
        if status.get("commit"):
            print(f"Git 提交：{status['commit']}；已推送：{status['pushed']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, FileNotFoundError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(2)
