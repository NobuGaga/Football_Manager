const { projectFiles } = require('../../data/project');
const { copyText } = require('../../utils/clipboard');

Page({
  data: {
    projectFiles,
    selectedIndex: 0,
    currentFile: projectFiles[0],
  },

  selectFile(event) {
    const selectedIndex = Number(event.currentTarget.dataset.index);
    this.setData({ selectedIndex, currentFile: projectFiles[selectedIndex] });
  },

  copyFilePath() {
    copyText(this.data.currentFile.path, '文件路径已复制');
  },

  copyTestCommand() {
    copyText('python3 -m unittest discover -s tests -v', '测试命令已复制');
  },
});
