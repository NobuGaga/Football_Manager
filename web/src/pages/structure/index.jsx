import { useState } from 'react';
import { Button, Text, View } from '@tarojs/components';
import { Notice, Page, SectionHeading } from '../../components/Page';
import { projectFiles } from '../../data/project';
import { boundedIndex } from '../../utils/report';
import { useCopy } from '../../utils/useCopy';

export default function Structure() {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const { clearCopyMessage, copyMessage, copyText } = useCopy();
  const currentFile = projectFiles[selectedIndex];

  return (
    <Page className='structure' copyMessage={copyMessage} onCopyMessageClear={clearCopyMessage}>
      <SectionHeading first kicker='05 / PROJECT STRUCTURE' title='每个模块，都有清晰职责' description='选择文件或目录，查看它在工程中的作用。' />
      <View className='file-card card'>
        <View className='file-card-head'><Text className='folder-mark'>▾</Text><Text className='mono'>Football_Manager/</Text><Text className='file-count'>{projectFiles.length} 项</Text></View>
        {projectFiles.map((item, index) => (
          <Button key={item.path} className={`file-row ${selectedIndex === index ? 'file-active' : ''}`} onClick={() => setSelectedIndex(boundedIndex(index, projectFiles.length))}>
            <Text className='file-bullet'>{item.type === '产物' ? '▱' : '·'}</Text>
            <Text className='file-path mono'>{item.path}</Text>
            <Text className='file-type'>{item.type}</Text>
          </Button>
        ))}
      </View>
      <View className='file-detail card'>
        <View className='file-detail-mark'>{'{ }'}</View>
        <View className='file-tag mono'>{currentFile.tag}</View>
        <View className='file-detail-title mono'>{currentFile.path}</View>
        <View className='file-detail-description'>{currentFile.description}</View>
        <View className='file-meta'><Text>模块类型</Text><Text>{currentFile.type}</Text></View>
        <View className='file-meta'><Text>工程约束</Text><Text>{currentFile.type === '产物' ? '按安全范围写回' : '职责单一，可独立验证'}</Text></View>
        <Button className='secondary-button copy-path' onClick={() => copyText(currentFile.path, '文件路径已复制')}>复制文件路径</Button>
      </View>

      <SectionHeading compact kicker='ENVIRONMENT' title='运行环境' />
      <View className='environment-card card'>
        <View><Text>RUNTIME</Text><Text className='environment-value'>Python 3.10+</Text></View>
        <View><Text>DATABASE</Text><Text className='environment-value'>SQLite</Text></View>
        <View><Text>FRONTEND</Text><Text className='environment-value'>Taro + React</Text></View>
        <View><Text>TARGETS</Text><Text className='environment-value'>H5 / 微信小程序</Text></View>
      </View>
      <Button className='primary-button test-button' onClick={() => copyText('python3 -m unittest discover -s tests -v', '测试命令已复制')}>复制测试命令</Button>
      <Notice mark='i'>本页面用于浏览工程信息，不直接读取本地数据库，也不会执行仓库命令或修改 Git 历史。</Notice>
    </Page>
  );
}
