import { useState } from 'react';
import { Button, ScrollView, Text, View } from '@tarojs/components';
import { Notice, Page, SectionHeading } from '../../components/Page';
import { flowSteps, tables } from '../../data/project';
import { boundedIndex } from '../../utils/report';

export default function Pipeline() {
  const [flowIndex, setFlowIndex] = useState(0);
  const currentFlow = flowSteps[flowIndex];
  const selectFlow = (index) => setFlowIndex(boundedIndex(index, flowSteps.length));

  return (
    <Page className='pipeline'>
      <SectionHeading first kicker='02 / DATA PIPELINE' title='从对阵，到可验证的结论' description='点击节点，查看每一步的输入、输出与异常边界。' />
      <ScrollView scrollX enhanced showScrollbar={false} className='flow-scroll'>
        <View className='flow-tabs'>
          {flowSteps.map((item, index) => (
            <Button key={item.short} className={`flow-tab ${flowIndex === index ? 'flow-active' : ''}`} onClick={() => selectFlow(index)}>
              <Text className='flow-number mono'>{String(index + 1).padStart(2, '0')}</Text>
              <Text className='flow-mark'>{item.mark}</Text>
              <Text className='flow-name'>{item.short}</Text>
            </Button>
          ))}
        </View>
      </ScrollView>

      <View className='flow-detail card'>
        <View className='detail-kicker mono'>{currentFlow.eyebrow} / {flowIndex + 1}</View>
        <View className='detail-title'>{currentFlow.title}</View>
        <View className='detail-lead'>{currentFlow.description}</View>
        <View className='detail-copy'>{currentFlow.detail}</View>
        <View className='flow-progress'>
          <View className='progress-track'><View className='progress-value' style={{ width: `${(flowIndex + 1) * 100 / flowSteps.length}%` }} /></View>
          <Text className='mono'>{flowIndex + 1} / {flowSteps.length}</Text>
        </View>
      </View>
      <View className='io-card card'>
        <View className='io-block'><Text className='io-label mono'>INPUT</Text><Text className='io-value'>{currentFlow.input}</Text></View>
        <View className='io-arrow'>↓</View>
        <View className='io-block io-output'><Text className='io-label mono'>OUTPUT</Text><Text className='io-value'>{currentFlow.output}</Text></View>
        <View className='io-safe'>✓ 来源、时间与状态始终保留</View>
      </View>
      {flowIndex < flowSteps.length - 1
        ? <Button className='primary-button next-button' onClick={() => selectFlow(flowIndex + 1)}>查看下一节点<Text className='button-arrow'>→</Text></Button>
        : <View className='complete-card'>✓ 已查看完整数据流水线</View>}

      <SectionHeading kicker='SQLITE SCHEMA' title='08 张核心数据表' description='职责清晰，关联可审计；历史快照和推断版本不被覆盖。' />
      <View className='tables-card card'>{tables.map((item) => <Text key={item} className='table-tag mono'>{item}</Text>)}</View>
      <Notice mark='i'>business_date 是体彩营业日；数据库存储 UTC 开赛时间，同时保留原始执行时区，避免跨日比赛错配。</Notice>
    </Page>
  );
}
