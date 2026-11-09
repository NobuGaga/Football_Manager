import { Text, View } from '@tarojs/components';
import { Notice, Page } from '../../components/Page';
import { dailyReport } from '../../data/daily-report';
import { decorateMatches } from '../../utils/report';

const matches = decorateMatches(dailyReport);

export default function Daily() {
  return (
    <Page className='daily'>
      <View className='section-heading daily-heading'>
        <Text className='section-kicker mono'>DAILY PROBABILITY REPORT</Text>
        <Text className='section-title'>每日推断</Text>
        <Text className='section-description'>{dailyReport.reportDate} · {dailyReport.matchCount} 场 / {dailyReport.predictionCount} 条策略 · 由 Python 每日任务打包</Text>
      </View>
      <Notice>{dailyReport.disclaimer} 数据完整度较低时仅作弱参考。</Notice>

      {matches.length > 0 ? (
        <View className='match-list'>
          {matches.map((item) => (
            <View key={item.strategyKey} className='match-card card'>
              <View className='match-topline'>
                <Text className='match-code mono'>{item.matchCode}</Text>
                <Text className='match-time mono'>{item.startTimeText}</Text>
              </View>
              <View className='match-league'>{item.league} · {item.aiModel} / {item.strategyCategory}</View>
              <View className='match-teams'>{item.homeTeam}<Text>VS</Text>{item.awayTeam}</View>
              <View className='tendency-row'><Text>最高概率倾向</Text><Text className='tendency-value'>{item.tendency}</Text></View>
              <View className='probability-grid'>
                <View><Text>主胜</Text><Text className='mono'>{item.homePercent}</Text></View>
                <View><Text>平局</Text><Text className='mono'>{item.drawPercent}</Text></View>
                <View><Text>客胜</Text><Text className='mono'>{item.awayPercent}</Text></View>
              </View>
              <View className='score-row'>
                <Text>比分参考</Text>
                <View className='score-values'>
                  {item.topScores.map((score) => <Text key={score.score} className='score-item mono'>{score.score} · {score.probabilityText}</Text>)}
                </View>
              </View>
              <View className='data-row'><Text>完整度 {item.completenessText}</Text><Text>{item.methodText}</Text></View>
              <View className='risk-text'>{item.primaryRisk}{item.remainingRiskCount > 0 && <Text> 等 {item.remainingRiskCount + 1} 项风险</Text>}</View>
            </View>
          ))}
        </View>
      ) : <View className='empty-card card'>暂无可展示的每日推断，请先运行每日任务。</View>}
      <View className='footer-note'>数据生成于 {dailyReport.generatedAt}{'\n'}本页面为打包时的报告快照，不实时连接数据库。</View>
    </Page>
  );
}
