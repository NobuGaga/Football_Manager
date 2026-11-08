# ChatGPT｜市场对照策略

```json
{
  "ai_model": "chatgpt",
  "model_name": "ChatGPT",
  "strategy_id": "chatgpt-market-comparison-v1",
  "strategy_category": "市场对照"
}
```

这是项目定义的 ChatGPT 策略分类，不代表调用或验证任何外部 AI 服务。

- 使用本地统计模型概率，并与官方对阵中已有的胜平负赔率作对照。
- 赔率仅用于观察市场差异，不重复加权，不改变已保存的统计模型概率。
- 同时列出模型最高概率方向和市场隐含方向；差异只标记为观察点，不直接升级为推荐。
- 缺少完整赔率、数据完整度不足或时间边界不合法时标记观望。
- 输出必须保留风险提示和概率免责声明。
