"""Kimi Claw／cron 每日编排：Git、赛果、推断、回测、摘要与安全推送。"""
from __future__ import annotations

import fcntl
import json
import subprocess
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from backtest import Backtester
from database import Database
from importers import FixtureImporter, ResultImporter
from miniprogram_export import export_miniprogram_report
from predictor import FootballPredictor
from reporter import ReportGenerator
from result_checker import ResultChecker
from scraper import SportteryScraper
from utils import utc_now


LOCAL_PRODUCT_PATHS = (
    "data/football.db",
    "reports",
    "rules",
    "docs",
    "README.md",
    "AGENTS.md",
    "web/generated/daily-report.js",
    "delivery/wechat-miniprogram/validation-20261108/compiled.zip",
)


class GitRepository:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()

    def _run(self, *args: str) -> str:
        process = subprocess.run(["git", *args], cwd=self.root, text=True,
                                 capture_output=True, timeout=120)
        if process.returncode:
            message = (process.stderr or process.stdout).strip()
            raise RuntimeError(f"Git 执行失败（{args[0]}）：{message}")
        return process.stdout.strip()

    def ensure_clean(self) -> None:
        self._run("rev-parse", "--is-inside-work-tree")
        dirty = self._run("status", "--porcelain")
        if dirty:
            raise RuntimeError("Git 工作区不干净；为避免覆盖人工修改，每日任务已中止：\n" + dirty)
        branch = self._run("rev-parse", "--abbrev-ref", "HEAD")
        if branch == "HEAD":
            raise RuntimeError("Git 处于 detached HEAD，不能执行每日流程")

    def pull(self) -> None:
        self._run("pull", "--rebase")

    def assert_local_products_untracked(self) -> None:
        tracked = self._run("ls-files", "--", *LOCAL_PRODUCT_PATHS)
        staged = self._run("diff", "--cached", "--name-only", "--diff-filter=ACMRT",
                           "--", *LOCAL_PRODUCT_PATHS)
        violations = []
        if tracked:
            violations.append("已跟踪：\n" + tracked)
        if staged:
            violations.append("已暂存：\n" + staged)
        if violations:
            raise RuntimeError("本地关键文件不得进入 Git 跟踪或暂存：\n" + "\n".join(violations))


class DailyAutomation:
    def __init__(self, repo_root: str | Path, db_path: str | Path | None = None,
                 output_dir: str | Path | None = None, frontend_dir: str | Path | None = None):
        self.repo_root = Path(repo_root).resolve()
        self.db_path = Path(db_path) if db_path else self.repo_root / "data" / "football.db"
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / "reports"
        self.frontend_data_path = (Path(frontend_dir) if frontend_dir
                                   else self.repo_root / "web") / "generated" / "daily-report.js"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.git = GitRepository(self.repo_root)

    def run(self, date: str | None = None, results_date: str | None = None,
            fixtures: str | None = None, results: str | None = None,
            refresh: bool = False, require_results: bool = False,
            pull: bool = False, push: bool = False, ai_model: str = "kimi") -> dict:
        match_date = date or datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
        day = datetime.strptime(match_date, "%Y-%m-%d").date()
        previous_date = results_date or (day - timedelta(days=1)).isoformat()
        datetime.strptime(previous_date, "%Y-%m-%d")
        with self._lock():
            if pull or push:
                self.git.ensure_clean()
            if pull:
                self.git.pull()
            Database(self.db_path).initialize()
            warnings: list[str] = []

            if results:
                result_info = ResultImporter(self.db_path).import_file(results)
            else:
                result_info = {"imported": 0, "errors": [], "skipped": True}
                warnings.append("未提供昨日赛果文件，赛果回填未执行")
            if result_info.get("errors"):
                raise RuntimeError(f"赛果导入存在关联错误，已停止后续推送：{result_info['errors'][:3]}")
            if result_info.get("unmatched"):
                warnings.append(f"{len(result_info['unmatched'])} 场赛果关联待人工确认")
            if require_results and (result_info.get("skipped") or result_info.get("imported", 0) == 0):
                raise RuntimeError("require-results 已启用，但昨日赛果没有可靠回填")
            checked = len(ResultChecker(self.db_path).check_all())
            from checklist import ChecklistGenerator
            from history import HistoryManager
            from strategy_engine import StrategyEngine, StrategyValidator

            checklist_generator = ChecklistGenerator(self.db_path, self.output_dir, ai_model=ai_model)
            checked_checklists = []
            if result_info.get("imported", 0):
                previous_checklist = checklist_generator.generate(previous_date)
                if previous_checklist["rows"]:
                    checked_checklists.append(previous_checklist["csv"])

            if fixtures:
                fixture_info = FixtureImporter(self.db_path).import_file(fixtures, default_date=match_date)
                if fixture_info.get("errors"):
                    raise RuntimeError(f"官方对阵存在导入错误：{fixture_info['errors'][:3]}")
                fixture_count = fixture_info["imported"]
            else:
                frame = SportteryScraper(self.db_path).fetch_daily_matches(match_date)
                fixture_count = len(frame)
            if fixture_count == 0:
                raise RuntimeError("没有导入有效官方对阵，已停止推断和推送")

            predictions = FootballPredictor(self.db_path, ai_model=ai_model).predict_all(match_date, refresh=refresh)
            reporter = ReportGenerator(self.db_path, self.output_dir)
            report_paths = reporter.generate_daily_report(match_date)
            strategy_result = StrategyEngine(self.db_path, self.output_dir, ai_model=ai_model).generate(match_date)
            checklist_result = checklist_generator.generate(match_date)
            validation_result = StrategyValidator(self.db_path, self.output_dir).generate(match_date)
            history_result = HistoryManager(self.db_path).export(self.output_dir / "足球预测历史数据.csv")
            frontend_data_path = export_miniprogram_report(report_paths["json"], self.frontend_data_path)
            backtest = Backtester(self.db_path).evaluate()
            backtest_path = self.output_dir / f"回测结果_{match_date}.json"
            backtest_path.write_text(json.dumps(backtest, ensure_ascii=False, indent=2), encoding="utf-8")

            status = {
                "status": "completed", "date": match_date, "results_date": previous_date,
                "generated_at": utc_now(), "fixtures": fixture_count, "ai_model": ai_model,
                "results_imported": result_info.get("imported", 0), "predictions_checked": checked,
                "new_predictions": len(predictions), "warnings": warnings,
                "reports": {key: self._relative(path) for key, path in report_paths.items()},
                "strategy_reports": {key: self._relative(strategy_result[key]) for key in ("json", "csv", "markdown")},
                "checklist": self._relative(checklist_result["csv"]),
                "checked_checklists": [self._relative(path) for path in checked_checklists],
                "strategy_validation": self._relative(validation_result["json"]),
                "history": self._relative(history_result["csv"]),
                "history_rows": history_result["rows"],
                "backtest": self._relative(backtest_path),
                "miniprogram_data": self._relative(frontend_data_path),
            }
            status_path = self.output_dir / f"每日运行状态_{match_date}.json"
            summary_path = self.output_dir / f"每日中文摘要_{match_date}.md"
            status["status_file"] = self._relative(status_path)
            status["summary_file"] = self._relative(summary_path)
            # push 仅为旧命令兼容保留；任何情况下都不提交或推送本地关键文件。
            self.git.assert_local_products_untracked()
            status["local_products_only"] = True
            status["commit"] = None
            status["pushed"] = False
            status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
            summary_path.write_text(self._markdown_summary(status, backtest), encoding="utf-8")
            return status

    @contextmanager
    def _lock(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.db_path.parent / ".daily_automation.lock"
        with lock_path.open("w") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("已有每日任务正在运行，本次不重复执行") from exc
            lock.write(utc_now())
            lock.flush()
            yield

    def _relative(self, path: str | Path) -> str:
        try:
            return str(Path(path).resolve().relative_to(self.repo_root))
        except ValueError:
            return str(Path(path).resolve())

    @staticmethod
    def _markdown_summary(status: dict, backtest: dict) -> str:
        overall = backtest.get("metrics", {}).get("全部联赛", {})
        lines = [f"# 足球经理的推断每日摘要（{status['date']}）", "",
                 f"- 官方对阵：{status['fixtures']} 场",
                 f"- 昨日赛果回填：{status['results_imported']} 场",
                 f"- 已核对推断版本：{status['predictions_checked']} 条",
                 f"- 今日新增推断：{status['new_predictions']} 条",
                 f"- 每日报告：{status['reports']['markdown']}",
                 f"- 双选与方向策略：{status['strategy_reports']['markdown']}",
                 f"- 预测核对表：{status['checklist']}",
                 f"- 历史数据：{status['history']}（{status['history_rows']} 场）",
                 f"- 策略校验：{status['strategy_validation']}",
                 f"- 跨端报告数据：{status['miniprogram_data']}",
                 f"- 回测结果：{status['backtest']}",
                 "- 本地关键文件：仅保存本地，未进入 Git 跟踪或暂存"]
        if overall.get("predictions"):
            lines += [f"- 胜平负准确率：{overall['outcome_accuracy']:.1%}",
                      f"- 首选／前三比分命中率：{overall['top1_score_accuracy']:.1%}／{overall['top3_score_accuracy']:.1%}",
                      f"- Log Loss／Brier：{overall['log_loss']:.4f}／{overall['brier_score']:.4f}"]
        else:
            lines.append("- 回测：暂无可评价的历史推断")
        lines += ["", "## 风险与待处理事项", ""]
        lines += [f"- {warning}" for warning in status["warnings"]] or ["- 无附加告警；仍需关注临场阵容和数据更新"]
        lines += ["", "> 本结果为概率分析，不构成收益保证或确定性投注建议。", ""]
        return "\n".join(lines)
