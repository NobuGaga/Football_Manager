"""Kimi Claw／cron 每日编排：Git、赛果、预测、回测、摘要与安全推送。"""
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
from predictor import FootballPredictor
from reporter import ReportGenerator
from result_checker import ResultChecker
from scraper import SportteryScraper
from utils import utc_now


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
            raise RuntimeError("Git 处于 detached HEAD，不能执行每日自动提交")

    def pull(self) -> None:
        self._run("pull", "--rebase")

    def commit_and_push(self, date: str, push: bool) -> str | None:
        # 只提交明确的数据产物，不暂存源码、凭证、缓存或人工未提交修改。
        self._run("add", "-f", "--", "data/football.db", "reports")
        if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=self.root).returncode == 0:
            return None
        self._run("-c", "user.name=Kimi Claw", "-c", "user.email=kimi-claw@localhost",
                  "commit", "-m", f"Daily football prediction: {date}")
        commit = self._run("rev-parse", "--short", "HEAD")
        if push:
            self._run("push")
        return commit


class DailyAutomation:
    def __init__(self, repo_root: str | Path, db_path: str | Path | None = None,
                 output_dir: str | Path | None = None):
        self.repo_root = Path(repo_root).resolve()
        self.db_path = Path(db_path) if db_path else self.repo_root / "data" / "football.db"
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / "reports"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.git = GitRepository(self.repo_root)

    def run(self, date: str | None = None, results_date: str | None = None,
            fixtures: str | None = None, results: str | None = None,
            enrich: bool = False, sync_results: bool = False,
            refresh: bool = False, require_results: bool = False,
            pull: bool = False, push: bool = False) -> dict:
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
            elif sync_results:
                from providers import APIFootballEnricher
                result_info = APIFootballEnricher(self.db_path).sync_results(previous_date)
            else:
                result_info = {"imported": 0, "errors": [], "skipped": True}
                warnings.append("未配置赛果文件或 API-Football，昨日赛果回填未执行")
            if result_info.get("errors"):
                raise RuntimeError(f"赛果导入存在关联错误，已停止后续推送：{result_info['errors'][:3]}")
            if result_info.get("unmatched"):
                warnings.append(f"{len(result_info['unmatched'])} 场赛果外部匹配待人工确认")
            if result_info.get("pending"):
                warnings.append(f"{result_info['pending']} 场外部比赛尚未完结")
            if require_results:
                missing_manual_results = bool(results) and result_info.get("imported", 0) == 0
                missing_api_results = (result_info.get("fixtures", 0) > 0 and result_info.get("imported", 0) == 0
                                       and not result_info.get("pending"))
                if result_info.get("skipped") or missing_manual_results or missing_api_results:
                    raise RuntimeError("require-results 已启用，但昨日赛果没有可靠回填")
            checked = len(ResultChecker(self.db_path).check_all())

            if fixtures:
                fixture_info = FixtureImporter(self.db_path).import_file(fixtures, default_date=match_date)
                if fixture_info.get("errors"):
                    raise RuntimeError(f"官方对阵存在导入错误：{fixture_info['errors'][:3]}")
                fixture_count = fixture_info["imported"]
            else:
                frame = SportteryScraper(self.db_path).fetch_daily_matches(match_date)
                fixture_count = len(frame)
            if fixture_count == 0:
                raise RuntimeError("没有导入有效官方对阵，已停止预测和推送")

            enrichment_info = None
            if enrich:
                from providers import APIFootballEnricher
                enrichment_info = APIFootballEnricher(self.db_path).enrich(match_date)
                if enrichment_info.get("errors"):
                    warnings.append(f"API-Football 有 {len(enrichment_info['errors'])} 场特征补充失败，使用现有数据继续")
                if enrichment_info.get("manual_review"):
                    warnings.append(f"{enrichment_info['manual_review']} 场赛前外部匹配待人工确认")

            predictions = FootballPredictor(self.db_path).predict_all(match_date, refresh=refresh)
            reporter = ReportGenerator(self.db_path, self.output_dir)
            report_paths = reporter.generate_daily_report(match_date)
            backtest = Backtester(self.db_path).evaluate()
            backtest_path = self.output_dir / f"回测结果_{match_date}.json"
            backtest_path.write_text(json.dumps(backtest, ensure_ascii=False, indent=2), encoding="utf-8")

            status = {
                "status": "completed", "date": match_date, "results_date": previous_date,
                "generated_at": utc_now(), "fixtures": fixture_count,
                "results_imported": result_info.get("imported", 0), "predictions_checked": checked,
                "new_predictions": len(predictions), "warnings": warnings,
                "reports": {key: self._relative(path) for key, path in report_paths.items()},
                "backtest": self._relative(backtest_path),
                "enrichment": enrichment_info,
            }
            status_path = self.output_dir / f"每日运行状态_{match_date}.json"
            summary_path = self.output_dir / f"每日中文摘要_{match_date}.md"
            status["status_file"] = self._relative(status_path)
            status["summary_file"] = self._relative(summary_path)
            status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
            summary_path.write_text(self._markdown_summary(status, backtest), encoding="utf-8")
            commit = self.git.commit_and_push(match_date, push) if push else None
            status["commit"] = commit
            status["pushed"] = bool(push and commit)
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
        lines = [f"# 足球预测每日摘要（{status['date']}）", "",
                 f"- 官方对阵：{status['fixtures']} 场",
                 f"- 昨日赛果回填：{status['results_imported']} 场",
                 f"- 已核对预测版本：{status['predictions_checked']} 条",
                 f"- 今日新增预测：{status['new_predictions']} 条",
                 f"- 每日报告：{status['reports']['markdown']}",
                 f"- 回测结果：{status['backtest']}"]
        if overall.get("predictions"):
            lines += [f"- 胜平负准确率：{overall['outcome_accuracy']:.1%}",
                      f"- 首选／前三比分命中率：{overall['top1_score_accuracy']:.1%}／{overall['top3_score_accuracy']:.1%}",
                      f"- Log Loss／Brier：{overall['log_loss']:.4f}／{overall['brier_score']:.4f}"]
        else:
            lines.append("- 回测：暂无可评价的历史预测")
        lines += ["", "## 风险与待处理事项", ""]
        lines += [f"- {warning}" for warning in status["warnings"]] or ["- 无附加告警；仍需关注临场阵容和数据更新"]
        lines += ["", "> 本结果为概率分析，不构成收益保证或确定性投注建议。", ""]
        return "\n".join(lines)
