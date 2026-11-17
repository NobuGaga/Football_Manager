function average(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function rounded(value) {
  return Math.round(value * 100) / 100;
}

class FrameRateController {
  constructor(now) {
    this.now = now;
    this.targetFps = 60;
    this.lastDrawTime = null;
    this.frameTimes = [];
    this.renderSamples = [];
    this.windowStartedAt = null;
    this.lastSampleAt = null;
  }

  shouldDraw(timestamp) {
    if (this.lastDrawTime === null || timestamp < this.lastDrawTime) {
      this.lastDrawTime = timestamp;
      return true;
    }
    if (timestamp - this.lastDrawTime + 1 < 1000 / this.targetFps) return false;
    this.lastDrawTime = timestamp;
    return true;
  }

  resetSamples(timestamp) {
    this.frameTimes = [];
    this.renderSamples = [];
    this.windowStartedAt = timestamp;
    this.lastSampleAt = timestamp;
  }

  recordFrame(timestamp, renderMs) {
    if (this.windowStartedAt === null || this.lastSampleAt === null || timestamp < this.lastSampleAt || timestamp - this.lastSampleAt > 1000) {
      this.resetSamples(timestamp);
    }
    this.lastSampleAt = timestamp;
    this.frameTimes.push(timestamp);
    this.renderSamples.push(Math.max(0, renderMs));
    if (this.frameTimes.length > 60) this.frameTimes.shift();
    if (this.renderSamples.length > 60) this.renderSamples.shift();

    const fps = this.frameTimes.length > 1
      ? (this.frameTimes.length - 1) * 1000 / (timestamp - this.frameTimes[0])
      : 0;
    const renderAvgMs = average(this.renderSamples);
    const windowMs = timestamp - this.windowStartedAt;
    if (this.targetFps === 60 && this.frameTimes.length >= 20 && (renderAvgMs > 14 || fps < 50)) {
      this.targetFps = 30;
      this.resetSamples(timestamp);
      return;
    }
    if (this.targetFps === 30 && windowMs >= 4000 && this.frameTimes.length >= 20 && renderAvgMs < 8 && fps >= 28) {
      this.targetFps = 60;
      this.resetSamples(timestamp);
      return;
    }
    if (windowMs >= 5000) this.resetSamples(timestamp);
  }

  snapshot() {
    const fps = this.frameTimes.length > 1
      ? (this.frameTimes.length - 1) * 1000 / (this.lastSampleAt - this.frameTimes[0])
      : 0;
    return {
      targetFps: this.targetFps,
      fps: rounded(fps),
      renderAvgMs: rounded(average(this.renderSamples)),
    };
  }
}

module.exports = { FrameRateController };
