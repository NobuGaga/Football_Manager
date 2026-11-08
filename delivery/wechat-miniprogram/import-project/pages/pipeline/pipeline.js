const { flowSteps, tables } = require('../../data/project');

Page({
  data: {
    flowSteps,
    tables,
    flowIndex: 0,
    currentFlow: flowSteps[0],
  },

  selectFlow(event) {
    const flowIndex = Number(event.currentTarget.dataset.index);
    this.setData({ flowIndex, currentFlow: flowSteps[flowIndex] });
  },

  nextFlow() {
    const flowIndex = Math.min(this.data.flowIndex + 1, flowSteps.length - 1);
    this.setData({ flowIndex, currentFlow: flowSteps[flowIndex] });
  },
});
