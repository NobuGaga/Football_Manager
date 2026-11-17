function average(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function rounded(value) {
  return Math.round(value * 100) / 100;
}

class RenderProfiler {
  constructor(now, enabled = false) {
    this.now = now;
    this.enabled = enabled;
    this.renderSamples = [];
    this.frameTimes = [];
    this.renderCount = 0;
    this.frameCount = 0;
    this.drawCalls = {};
    this.lastLog = this.now();
  }

  render(duration) {
    if (!this.enabled) return;
    this.renderCount += 1;
    this.renderSamples.push(Math.max(0, duration));
    if (this.renderSamples.length > 120) this.renderSamples.shift();
  }

  frame(timestamp) {
    if (!this.enabled || !Number.isFinite(timestamp)) return;
    this.frameCount += 1;
    this.frameTimes.push(timestamp);
    if (this.frameTimes.length > 120) this.frameTimes.shift();
  }

  draw(type) {
    if (!this.enabled) return;
    this.drawCalls[type] = (this.drawCalls[type] || 0) + 1;
  }

  snapshot() {
    const sorted = [...this.renderSamples].sort((a, b) => a - b);
    const intervals = this.frameTimes.slice(1).map((time, index) => time - this.frameTimes[index]).filter((value) => value > 0);
    return {
      renders: this.renderCount,
      frames: this.frameCount,
      renderAvgMs: rounded(average(this.renderSamples)),
      renderP95Ms: rounded(sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))] : 0),
      renderMaxMs: rounded(sorted[sorted.length - 1] || 0),
      fps: rounded(intervals.length ? 1000 / average(intervals) : 0),
      drawCalls: { ...this.drawCalls },
    };
  }

  maybeLog(timestamp) {
    if (!this.enabled || !this.renderSamples.length || timestamp - this.lastLog < 1000) return;
    this.lastLog = timestamp;
    console.log(`[RenderProfile] ${JSON.stringify(this.snapshot())}`);
  }
}

module.exports = { RenderProfiler };
