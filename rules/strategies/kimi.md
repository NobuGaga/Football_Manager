# Kimi｜稳健概率策略

```json
{
  "ai_model": "kimi",
  "model_name": "Kimi",
  "strategy_id": "kimi-probability-v1",
  "strategy_category": "稳健概率"
}
```

适用于现有及默认的 Kimi 每日预测分类。

- 使用本地统计模型输出的三向概率、预期进球和比分分布，不调用外部足球数据 API。
- 先展示数据完整度、特征来源和风险，再表达最高概率方向。
- `odds_only_fallback`、低完整度或关键字段冲突只能作为弱参考，必要时标记观望。
- 不把历史命中率、小样本结果或最高概率写成确定预测或收益承诺。
- 每日叙事与频道策略遵循 `rules/football_manager_policy.md`。
