import { Text, View } from '@tarojs/components';
import { Notice, Page } from '../../components/Page';
import { dailyReport } from '../../data/daily-report';
import { decorateMatches } from '../../utils/report';

const matches = decorateMatches(dailyReport);

function ProbabilityBar({ item }) {
  const segments = [
    { key: 'home', label: '主胜', value: item.probabilities.home_win },
    { key: 'draw', label: '平局', value: item.probabilities.draw },
    { key: 'away', label: '客胜', value: item.probabilities.away_win },
  ];
  return (
    <View className='probability-bar'>
      {segments.map((segment) => (
        <View
          key={segment.key}
          className={`probability-segment ${segment.key} ${item.tendency.startsWith(segment.label) ? 'active' : ''}`}
          style={{ width: `${segment.value * 100}%` }}
        />
      ))}
    </View>
  );
}

function ProbabilityValues({ item, compact = false }) {
  const values = [
    ['主胜', item.homePercent],
    ['平局', item.drawPercent],
    ['客胜', item.awayPercent],
  ];
  return (
    <View className={compact ? 'compact-probabilities' : 'probability-grid'}>
      {values.map(([label, value]) => (
        <View key={label} className={item.tendency.startsWith(label) ? 'highest' : ''}>
          <Text>{label}</Text><Text className='mono'>{value}</Text>
        </View>
      ))}
    </View>
  );
}

function MatchDetails({ item }) {
  return (
    <View className='match-details'>
      <View className='score-row'>
        <Text>比分参考</Text>
        <View className='score-values'>
          {item.topScores.map((score) => <Text key={score.score} className='score-item mono'>{score.score} · {score.probabilityText}</Text>)}
        </View>
      </View>
      <View className='data-row'>
        <Text className='completeness-text'>完整度 {item.completenessText}</Text>
        <Text className='method-text'>{item.methodText}</Text>
      </View>
      <View className='risk-text'>{item.primaryRisk}{item.remainingRiskCount > 0 && <Text> 等 {item.remainingRiskCount + 1} 项风险</Text>}</View>
    </View>
  );
}

function MatchTopline({ item }) {
  return (
    <View className='match-heading'>
      <View className='match-topline'>
        <Text className='match-code'>{item.matchCode}</Text>
        <Text className='match-league'>{item.league}</Text>
        <Text className='match-time mono'>{item.startTimeText}</Text>
      </View>
      <View className='match-model'>{item.aiModel} / {item.strategyCategory}</View>
    </View>
  );
}

function MatchMain({ item, compact = false }) {
  return (
    <View className={`match-main ${compact ? 'compact-main' : 'featured-main'}`}>
      <View className='match-teams-scan'>
        <Text>{item.homeTeam}</Text><Text>{item.awayTeam}</Text>
      </View>
      <View className='match-tendency-scan'>
        <Text className='tendency-label'>最高 · {item.tendencyLabel}</Text>
        <Text className='tendency-value mono'>{item.tendencyPercent}</Text>
      </View>
    </View>
  );
}

function MatchCard({ item, compact = false }) {
  return (
    <View className={`match-card card ${compact ? 'match-compact' : 'match-featured'}`}>
      <MatchTopline item={item} />
      <MatchMain item={item} compact={compact} />
      <ProbabilityBar item={item} />
      <ProbabilityValues item={item} compact={compact} />
      <MatchDetails item={item} />
    </View>
  );
}

function FeaturedMatch({ item }) {
  return <MatchCard item={item} />;
}

function CompactMatch({ item }) {
  return <MatchCard item={item} compact />;
}

export default function Daily() {
  return (
    <Page className='daily'>
      <View className='section-heading daily-heading'>
        <Text className='section-kicker mono'>MATCHDAY PROBABILITY REPORT</Text>
        <Text className='section-title'>每日推断</Text>
        <Text className='section-description'>{dailyReport.reportDate} · {dailyReport.matchCount} 场 / {dailyReport.predictionCount} 条策略 · 由 Python 每日任务打包</Text>
      </View>
      <Notice>{dailyReport.disclaimer} 数据完整度较低时仅作弱参考。</Notice>

      {matches.length > 0 ? (
        <View className='match-list'>
          {matches.map((item, index) => index === 0
            ? <FeaturedMatch key={item.strategyKey} item={item} />
            : <CompactMatch key={item.strategyKey} item={item} />)}
        </View>
      ) : <View className='empty-card card'>暂无可展示的每日推断，请先运行每日任务。</View>}
      <View className='footer-note'>数据生成于 {dailyReport.generatedAt}{'\n'}本页面为打包时的报告快照，不实时连接数据库。</View>
    </Page>
  );
}
