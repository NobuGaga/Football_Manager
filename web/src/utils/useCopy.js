import { useEffect, useState } from 'react';
import { writeClipboard } from './clipboard-core';
import { clipboardPlatform } from './clipboard-platform';

export function useCopy() {
  const [request, setRequest] = useState(null);
  const [copyMessage, setCopyMessage] = useState('');

  useEffect(() => {
    if (!request) return undefined;
    let active = true;
    setCopyMessage('正在复制…');
    try {
      writeClipboard(clipboardPlatform, request.text).then((success) => {
        if (active) setCopyMessage(success ? request.title : '复制失败，请稍后重试');
      }, () => {
        if (active) setCopyMessage('复制失败，请稍后重试');
      });
    } catch {
      if (active) setCopyMessage('复制失败，请稍后重试');
    }
    return () => { active = false; };
  }, [request]);

  function copyText(text, title = '已复制到剪贴板') {
    setRequest({ text, title });
  }

  function clearCopyMessage() {
    setCopyMessage('');
  }

  return { clearCopyMessage, copyMessage, copyText };
}
