import { Text, View } from '@tarojs/components';
import Taro from '@tarojs/taro';
import { Page } from '../../components/Page';
import { dailyReport } from '../../../generated/daily-report';
import { decorateMatches } from '../../utils/report';

const matches = decorateMatches(dailyReport);
const reportDateText = `${dailyReport.reportDate.slice(5).replace('-', ' 月 ')} 日`;

function TeamBadge({ name }) {
  return <View className='team-badge'>{Array.from(name || '待')[0]}</View>;
}

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

function ProbabilityValues({ item }) {
  const values = [
    ['主胜', item.homePercent],
    ['平局', item.drawPercent],
    ['客胜', item.awayPercent],
  ];
  return (
    <View className='compact-probabilities'>
      {values.map(([label, value]) => (
        <View key={label} className={item.tendency.startsWith(label) ? 'highest' : ''}>
          <Text>{label}</Text><Text className='mono'>{value}</Text>
        </View>
      ))}
    </View>
  );
}

function MatchDetails({ item, compact = false }) {
  return (
    <View className='match-details'>
      <View className='score-row'>
        <Text>比分参考</Text>
        <View className='score-values'>
          {item.topScores.map((score) => <Text key={score.score} className='score-item mono'>{score.score} · {score.probabilityText}</Text>)}
        </View>
      </View>
      <View className='data-row'>
        <View className='completeness-block'>
          <Text className='completeness-text'>完整度 {item.completenessText}</Text>
          {!compact && <View className='completeness-track'><View className='completeness-fill' style={{ width: item.completenessText }} /></View>}
        </View>
        <Text className='method-text'>{item.methodText}</Text>
      </View>
      <View className='risk-text'><Text className='risk-mark'>!</Text>{item.primaryRisk}{item.remainingRiskCount > 0 && <Text> 等 {item.remainingRiskCount + 1} 项风险</Text>}</View>
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

function Scoreboard({ item }) {
  return (
    <View className='scoreboard'>
      <View className='scoreboard-team'><TeamBadge name={item.homeTeam} /><Text>{item.homeTeam}</Text></View>
      <View className='scoreboard-center'>
        <Text className='scoreboard-vs mono'>VS</Text>
        <Text className='scoreboard-label'>最高倾向</Text>
        <Text className='tendency-pill'>{item.tendencyLabel}</Text>
      </View>
      <View className='scoreboard-team'><TeamBadge name={item.awayTeam} /><Text>{item.awayTeam}</Text></View>
    </View>
  );
}

function FeaturedProbability({ item }) {
  const others = [
    ['主胜', '主', item.homePercent],
    ['平局', '平', item.drawPercent],
    ['客胜', '客', item.awayPercent],
  ].filter(([label]) => label !== item.tendencyLabel);
  return (
    <View className='featured-probability'>
      <View className='featured-tendency'><Text>{item.tendencyLabel}</Text><Text className='mono'>{item.tendencyPercent}</Text></View>
      <Text className='featured-other mono'>{others.map(([, label, value]) => `${label} ${value}`).join('  /  ')}</Text>
    </View>
  );
}

function MatchMain({ item }) {
  return (
    <View className='match-main compact-main'>
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
      {compact ? <MatchMain item={item} /> : <><Scoreboard item={item} /><FeaturedProbability item={item} /></>}
      <ProbabilityBar item={item} />
      {compact && <ProbabilityValues item={item} />}
      <MatchDetails item={item} compact={compact} />
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
      <View className='daily-header'>
        <View className='daily-back' role='button' aria-label='返回概览' onClick={() => Taro.navigateBack()}>←</View>
        <Text className='daily-title'>今日比赛中心</Text>
        <View className='daily-date'>{reportDateText}</View>
      </View>
      <View className='daily-summary'><Text>{dailyReport.matchCount} 场</Text><Text>/ {dailyReport.predictionCount} 条策略</Text></View>
      <View className='daily-notice'>
        <Text className='daily-notice-title'>概率估计仅供分析</Text>
        <Text className='daily-notice-text'>低完整度仅作弱参考，不构成收益保证或确定性投注建议。</Text>
      </View>

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
