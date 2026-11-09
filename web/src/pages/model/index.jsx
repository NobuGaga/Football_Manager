import { Button, Text, View } from '@tarojs/components';
import { Notice, Page, SectionHeading } from '../../components/Page';
import { metrics, priorities } from '../../data/project';
import { useCopy } from '../../utils/useCopy';

export default function Model() {
  const { clearCopyMessage, copyMessage, copyText } = useCopy();

  return (
    <Page className='model' copyMessage={copyMessage} onCopyMessageClear={clearCopyMessage}>
      <SectionHeading first kicker='03 / MODEL & EVALUATION' title='输出概率，也输出不确定性' description='模型给出可复现基线，回测负责检验长期概率质量。' />
      <View className='formula-card'>
        <View className='formula-topline mono'><Text>PROBABILITY ENGINE</Text><Text>poisson-dc-v1</Text></View>
        <View className='formula-title'>从预期进球开始</View>
        <View className='formula-description'>基础比分概率由双方独立 Poisson 分布相乘，再对低比分进行 Dixon–Coles 修正。</View>
        <View className='formula-box'>
          <Text className='formula-caption'>基础比分概率</Text>
          <Text className='formula-main mono'>P(H=h, A=a)</Text>
          <Text className='formula-equation mono'>= Poisson(h; λhome) × Poisson(a; λaway)</Text>
        </View>
        <View className='correction-title'>低比分修正与归一化</View>
        <View className='correction-copy'>0:0、0:1、1:0、1:1 使用 rho 修正，在 0..10 球网格归一化。</View>
        <View className='formula-tags'><Text>三向概率</Text><Text>双方预期进球</Text><Text>前 5 个比分</Text></View>
      </View>

      <SectionHeading compact kicker='EXPECTED GOALS' title='预期进球的估计顺序' />
      <View className='priority-card card'>
        {priorities.map((item) => (
          <View key={item.id} className='priority-item'>
            <View className='priority-number mono'>{item.id}</View>
            <View className='priority-copy'><Text className='priority-title'>{item.title}</Text><Text className='priority-detail'>{item.detail}</Text></View>
          </View>
        ))}
      </View>

      <View className='boundary-card card'>
        <View className='boundary-kicker mono'>TIME INTEGRITY</View>
        <View className='boundary-title'>只有合法的时间链，才进入回测</View>
        <View className='boundary-copy'>防止未来数据泄漏，也避免赛后信息重写赛前判断。</View>
        <Button className='equation-button' onClick={() => copyText('input_as_of <= predicted_at <= start_time_utc', '时间边界已复制')}>
          <Text className='mono'>input_as_of</Text><Text>≤</Text><Text className='mono'>predicted_at</Text><Text>≤</Text><Text className='mono'>start_time_utc</Text>
        </Button>
        <View className='equation-hint'>点击复制时间边界表达式</View>
      </View>

      <SectionHeading kicker='ROLLING BACKTEST' title='评价概率，而非迷信单次命中' description='总体与分联赛分别统计，只使用已保存推断和真实赛果。' />
      <View className='metrics-grid'>
        {metrics.map((item, index) => (
          <View key={item.name} className='metric-card card'>
            <Text className='metric-number mono'>{String(index + 1).padStart(2, '0')}</Text>
            <Text className='metric-name'>{item.name}</Text>
            <Text className='metric-label'>{item.label}</Text>
          </View>
        ))}
      </View>
      <Notice>这是一套可运行基线，不宣称已完成分联赛参数训练。升班马、小样本、杯赛与阵容不确定信息应通过特征和风险标记体现。</Notice>
    </Page>
  );
}
