const { commandModes, automationSteps, safetyRules } = require('../../data/project');
const { copyText } = require('../../utils/clipboard');

Page({
  data: {
    commandModes,
    automationSteps,
    safetyRules,
    commandMode: 'daily',
    currentCommand: commandModes[0],
  },

  selectCommand(event) {
    const commandMode = event.currentTarget.dataset.mode;
    const currentCommand = commandModes.find((mode) => mode.id === commandMode);
    this.setData({ commandMode, currentCommand });
  },

  copyCommand() {
    copyText(this.data.currentCommand.command, '运行命令已复制');
  },
});
