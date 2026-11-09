import { useEffect } from 'react';
import { Text, View } from '@tarojs/components';

export function Page({ className = '', copyMessage = '', onCopyMessageClear = () => {}, children }) {
  useEffect(() => {
    if (!copyMessage || copyMessage === '正在复制…') return undefined;
    const timer = setTimeout(onCopyMessageClear, 2200);
    return () => clearTimeout(timer);
  }, [copyMessage, onCopyMessageClear]);

  return (
    <View className={`page ${className}`}>
      {children}
      {copyMessage && <View className='copy-toast' role='status'>{copyMessage}</View>}
    </View>
  );
}

export function SectionHeading({ kicker, title, description, compact = false, first = false }) {
  const classNames = [
    'section-heading',
    compact ? 'compact-heading' : '',
    first ? 'first-heading' : '',
  ].filter(Boolean).join(' ');
  return (
    <View className={classNames}>
      <Text className='section-kicker mono'>{kicker}</Text>
      <Text className={`section-title ${compact ? 'small-title' : ''}`}>{title}</Text>
      {description && <Text className='section-description'>{description}</Text>}
    </View>
  );
}

export function Notice({ mark = '!', children }) {
  return (
    <View className='notice'>
      <View className='notice-mark'>{mark}</View>
      <View className='notice-text'>{children}</View>
    </View>
  );
}
