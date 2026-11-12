import { useState } from 'react';
import { Button, Text, View } from '@tarojs/components';
import { Notice, Page, SectionHeading } from '../../components/Page';
import { automationSteps, commandModes, safetyRules } from '../../data/project';
import { useCopy } from '../../utils/useCopy';

export default function Automation() {
  const [commandMode, setCommandMode] = useState('daily');
  const { clearCopyMessage, copyMessage, copyText } = useCopy();
  const currentCommand = commandModes.find((item) => item.id === commandMode);

  return (
    <Page className='automation' copyMessage={copyMessage} onCopyMessageClear={clearCopyMessage}>
      <SectionHeading first kicker='04 / DAILY AUTOMATION' title='每天一条安全、完整的执行链' description='Kimi Claw 定时执行并推送预测策略，GitHub 仅保存项目版本。' />
      <View className='command-card card'>
        <View className='command-tabs'>
          {commandModes.map((item) => (
            <Button key={item.id} className={`command-tab ${commandMode === item.id ? 'command-active' : ''}`} onClick={() => setCommandMode(item.id)}>{item.label}</Button>
          ))}
        </View>
        <View className='command-content'>
          <View className='command-badge'>{currentCommand.badge}</View>
          <View className='command-title'>{currentCommand.title}</View>
          <View className='command-description'>{currentCommand.description}</View>
          <View className='terminal-window'>
            <View className='terminal-head'><Text className='mono'>TERMINAL</Text><Button className='copy-button' onClick={() => copyText(currentCommand.command, '运行命令已复制')}>复制</Button></View>
            <Text className='terminal-code mono' selectable>{currentCommand.command}</Text>
          </View>
        </View>
      </View>
      <View className='command-notice'><Notice>正式任务要求 Git 工作区干净；关联失败时立即中止，不上传半成品。本页面只展示命令，不会在你的设备上执行。</Notice></View>

      <SectionHeading compact kicker='DAILY RUNBOOK' title='每日执行顺序' />
      <View className='runbook-card card'>
        {automationSteps.map((item, index) => (
          <View key={item} className='runbook-step'>
            <View className='step-number mono'>{index + 1}</View>
            <View className='step-line' />
            <Text className='step-text'>{item}</Text>
          </View>
        ))}
      </View>

      <SectionHeading compact kicker='SAFETY BOUNDARIES' title='自动化安全边界' />
      <View className='safety-list'>
        {safetyRules.map((item) => (
          <View key={item.title} className='safety-card card'>
            <View className='safety-mark'>{item.mark}</View>
            <View className='safety-copy'><Text className='safety-title'>{item.title}</Text><Text className='safety-description'>{item.description}</Text></View>
          </View>
        ))}
      </View>
    </Page>
  );
}
