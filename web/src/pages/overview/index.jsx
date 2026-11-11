import { Button, Text, View } from '@tarojs/components';
import Taro from '@tarojs/taro';
import { Notice, Page, SectionHeading } from '../../components/Page';
import { principles, skillTestText, stats } from '../../data/project';
import { createNavigator } from '../../utils/navigation-core';
import { createMatrix } from '../../utils/report';
import { useCopy } from '../../utils/useCopy';

const matrix = createMatrix();
const { goDaily, goPipeline } = createNavigator(Taro);

export default function Overview() {
  const { clearCopyMessage, copyMessage, copyText } = useCopy();

  return (
    <Page className='overview' copyMessage={copyMessage} onCopyMessageClear={clearCopyMessage}>
      <View className='brand-row'>
        <View className='brand-mark'>FM</View>
        <View className='brand-copy'>
          <Text className='brand-name'>足球经理的推断</Text>
          <Text className='brand-subtitle mono'>FOOTBALL INTELLIGENCE</Text>
        </View>
        <View className='baseline-badge'>比赛中心</View>
      </View>

      <View className='hero-layout'>
        <View className='hero-copy'>
          <View className='hero-eyebrow mono'>MATCHDAY · 多联赛概率分析</View>
          <View className='hero-title'>让每一次推断，<Text>都有迹可循。</Text></View>
          <View className='hero-description'>看见概率，也看见不确定性。从官方对阵到赛后回测，保留完整证据与安全边界。</View>
          <View className='skill-test-banner'><Text>{skillTestText}</Text></View>
          <View className='hero-actions'>
            <Button className='primary-button' onClick={goDaily}>进入今日比赛中心<Text className='button-arrow'>→</Text></Button>
            <Button className='secondary-button' onClick={goPipeline}>探索数据流水线</Button>
            <Button className='secondary-button' onClick={() => copyText('bash scripts/daily_automation.sh', '每日命令已复制')}>复制每日命令</Button>
          </View>
        </View>

        <View className='model-card card'>
          <View className='pitch-lines' />
          <View className='card-topline'>
            <View>
              <View className='model-caption mono'>SCORE MATRIX</View>
              <View className='model-name mono'>poisson-dc-v1</View>
            </View>
            <View className='outline-badge'>模型 V1 · 可复现</View>
          </View>
          <View className='matrix-heading'>
            <Text>比分概率矩阵</Text>
            <Text className='matrix-note'>0—10 球网格 · 示意</Text>
          </View>
          <View className='score-matrix'>
            {matrix.map((item) => (
              <View key={item.id} className={`matrix-cell ${item.hot ? 'matrix-hot' : ''}`} style={{ opacity: item.opacity }} />
            ))}
          </View>
          <View className='model-features'>
            <View><Text className='feature-mark'>P</Text><Text>完整概率分布</Text></View>
            <View><Text className='feature-mark'>V</Text><Text>追加而非覆盖</Text></View>
          </View>
        </View>
      </View>

      <View className='stats-grid'>
        {stats.map((item, index) => (
          <View key={item.label} className={`stat-card ${index % 2 === 1 ? 'stat-even' : ''} ${index >= stats.length - 2 ? 'stat-last-row' : ''} ${index === 0 ? 'stat-first' : ''}`}>
            <View className='stat-value mono'>{item.value}<Text>{item.unit}</Text></View>
            <View className='stat-label'>{item.label}</View>
            <View className='stat-detail'>{item.detail}</View>
          </View>
        ))}
      </View>

      <SectionHeading kicker='01 / CORE PRINCIPLES' title='四条核心设计原则' description='保存推断结果，也保存得出结果所需的上下文。' />
      <View className='principle-list'>
        {principles.map((item) => (
          <View key={item.number} className='principle-card card'>
            <View className='principle-top'><Text className='principle-number mono'>{item.number}</Text><Text className='principle-mark'>{item.mark}</Text></View>
            <View className='principle-title'>{item.title}</View>
            <View className='principle-description'>{item.description}</View>
          </View>
        ))}
      </View>
      <Notice>足球经理的推断基于赛前数据与概率模型，不构成收益保证或确定性投注建议。信息不足时，明确标记数据缺失与不确定性。</Notice>
      <View className='footer-note'>Python CLI · SQLite · Poisson / Dixon–Coles{'\n'}Taro React H5 · 微信小程序</View>
    </Page>
  );
}
