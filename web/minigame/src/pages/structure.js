module.exports = function renderStructure(app, y) {
  const { painter: p, colors: c, width, project } = app;
  const x = app.padding;
  const w = width - app.padding * 2;
  y = app.section(y, '05 / PROJECT STRUCTURE', '每个模块，都有清晰职责', '选择文件或目录，查看它在工程中的作用。');

  const rowH = 46;
  const listH = project.projectFiles.length * rowH + 52;
  app.card(y, listH);
  p.text(`▾  Football_Manager/`, x + 14, y + 15, { size: 12, weight: 'bold', mono: true });
  p.text(`${project.projectFiles.length} 项`, x + w - 14, y + 15, { size: 11, color: c.muted, align: 'right' });
  project.projectFiles.forEach((item, index) => {
    const ry = y + 46 + index * rowH;
    app.button(`${item.type === '产物' ? '▱' : '·'}  ${item.path}`, x + 10, ry, w - 20, 40, { type: 'file', index }, { selected: app.selectedFile === index, file: true });
    p.text(item.type, x + w - 24, ry + 13, { size: 10, color: app.selectedFile === index ? c.accent : c.muted, align: 'right' });
  });
  y += listH + 12;

  const current = project.projectFiles[app.selectedFile];
  const inner = w - 32;
  const pathH = p.paragraphHeight(current.path, inner, { size: 18, weight: 'bold', mono: true, lineHeight: 27 });
  const descH = p.paragraphHeight(current.description, inner, { size: 13, color: c.secondary, lineHeight: 21 });
  const detailH = 174 + pathH + descH;
  app.card(y, detailH);
  p.text(`{ }  ${current.tag}`, x + 16, y + 16, { size: 11, color: c.accent, mono: true });
  p.paragraph(current.path, x + 16, y + 46, inner, { size: 18, weight: 'bold', mono: true, lineHeight: 27 });
  let cursor = y + 46 + pathH + 10;
  p.paragraph(current.description, x + 16, cursor, inner, { size: 13, color: c.secondary, lineHeight: 21 });
  cursor += descH + 14;
  p.text(`模块类型  ${current.type}`, x + 16, cursor, { size: 11, color: c.muted });
  cursor += 24;
  p.text(`工程约束  ${current.type === '产物' ? '仅保存本地，不进入 Git' : '职责单一，可独立验证'}`, x + 16, cursor, { size: 11, color: c.muted });
  app.button('复制文件路径', x + 16, y + detailH - 56, inner, 42, { type: 'copy', text: current.path, success: '文件路径已复制' });
  y += detailH + 20;

  y = app.section(y, 'ENVIRONMENT', '运行环境', '数据处理、H5 与小游戏各自保持清晰边界。');
  const items = [
    ['RUNTIME', 'Python 3.10+'],
    ['DATABASE', 'SQLite'],
    ['FRONTEND', 'Taro + Canvas'],
    ['TARGETS', 'H5 / minigame'],
  ];
  const envW = (w - 10) / 2;
  items.forEach((item, index) => {
    const ex = x + (index % 2) * (envW + 10);
    const ey = y + Math.floor(index / 2) * 76;
    p.roundRect(ex, ey, envW, 66, 12, c.card, c.border);
    p.text(item[0], ex + 12, ey + 11, { size: 9, color: c.muted, mono: true });
    p.text(item[1], ex + 12, ey + 34, { size: 13, weight: 'bold', color: c.accent });
  });
  y += 164;
  y = app.button('复制测试命令', x, y, w, 46, { type: 'copy', text: 'python3 -m unittest discover -s tests -v', success: '测试命令已复制' }, { primary: true }) + 14;
  return app.notice(y, '本页面用于浏览工程信息，不直接读取本地数据库，也不会执行仓库命令或修改 Git 历史。', 'i');
};
