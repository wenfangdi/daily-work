let all_data = [];
context.panel.data.series.forEach((s) => {
  const f_id = s.fields.find(f => f.name === 'id_block');
  const f_step = s.fields.find(f => f.name === 'd_step');
  const f_usl = s.fields.find(f => f.name === "product_upper_spec_limit");
  const f_spec = s.fields.find(f => f.name === "spec_available");
  const f_v = s.fields.find(f => f.name === "v");
  const f_std = s.fields.find(f => f.name === "stddev_v");

  for (let i = 0; i < s.length; i++) {
    all_data.push({
      id_block: f_id ? f_id.values[i] : null,
      stepUsl: `${f_step ? f_step.values[i] : ''}  USL: ${f_usl ? f_usl.values[i] : ''}`,
      usl: f_usl ? f_usl.values[i] : null,
      spec_available: f_spec ? f_spec.values[i] : null,
      v: f_v ? f_v.values[i] : null,
      stddev_v: f_std ? f_std.values[i] : null,
    });
  }
});

// Group data by step + USL
const groups = all_data.reduce((acc, item) => {
  const key = item.stepUsl;
  if (!acc[key]) acc[key] = [];
  acc[key].push(item);
  return acc;
}, {});

// Helper function to build dynamic SVG Histogram + Gaussian Fit Curve for tooltips
function generateHistogramWithGaussianSVG(values, binCount = 10) {
  const validValues = values.filter(v => v !== null && !isNaN(v));
  if (validValues.length === 0) return '<div style="color:#aaa;">No raw data available</div>';

  const n = validValues.length;
  const min = Math.min(...validValues);
  const max = Math.max(...validValues);

  if (min === max) {
    return `<div style="font-size:11px; color:#ccc;">All values are equal: ${min.toFixed(2)}</div>`;
  }

  // Calculate Mean (mu) and Standard Deviation (sigma)
  const mean = validValues.reduce((sum, v) => sum + v, 0) / n;
  const variance = validValues.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / (n > 1 ? n - 1 : 1);
  const stdDev = Math.sqrt(variance);

  // Calculate Bins
  const binWidth = (max - min) / binCount;
  const bins = new Array(binCount).fill(0);

  validValues.forEach(val => {
    let index = Math.floor((val - min) / binWidth);
    if (index >= binCount) index = binCount - 1;
    bins[index]++;
  });

  const maxCount = Math.max(...bins, 1);
  const width = 270; // <--- Increased from 210 to match tooltip width
  const height = 100;
  const chartBottom = height - 18;
  const maxBarHeight = height - 30;
  const barWidth = width / binCount;

  // Render Histogram Bars
  let barsSVG = '';
  bins.forEach((count, i) => {
    const barHeight = (count / maxCount) * maxBarHeight;
    const x = i * barWidth;
    const y = chartBottom - barHeight;

    barsSVG += `
      <rect x="${x + 1}" y="${y}" width="${Math.max(barWidth - 2, 1)}" height="${barHeight}" fill="#5470c6" opacity="0.6" rx="1" />
    `;
  });

  // Calculate Gaussian Curve Points
  // Normal PDF formula: f(x) = (1 / (stdDev * sqrt(2 * PI))) * exp(-0.5 * ((x - mean) / stdDev)^2)
  // Scaled frequency height = n * binWidth * PDF(x)
  let pathPoints = [];
  const steps = 50; // Points resolution for smooth curve

  if (stdDev > 0) {
    for (let i = 0; i <= steps; i++) {
      const xVal = min + (i / steps) * (max - min);
      const pdf = (1 / (stdDev * Math.sqrt(2 * Math.PI))) * Math.exp(-0.5 * Math.pow((xVal - mean) / stdDev, 2));
      const expectedCount = n * binWidth * pdf;

      const px = (i / steps) * width;
      const py = chartBottom - (expectedCount / maxCount) * maxBarHeight;

      // Clamp Y point within plot boundary
      const clampedY = Math.max(5, Math.min(chartBottom, py));
      pathPoints.push(`${i === 0 ? 'M' : 'L'} ${px.toFixed(1)} ${clampedY.toFixed(1)}`);
    }
  }

  const gaussianPathSVG = stdDev > 0 ? `
    <path d="${pathPoints.join(' ')}" fill="none" stroke="#ee6666" stroke-width="2" />
  ` : '';

  return `
    <svg width="100%" height="${height}" viewBox="0 0 ${width} ${height}" style="margin-top:6px; display:block;"> 
      <!-- Histogram Bars -->
      ${barsSVG}
      <!-- Gaussian Bell Curve Overlay -->
      ${gaussianPathSVG}
      <!-- Baseline and Labels -->
      <line x1="0" y1="${chartBottom}" x2="${width}" y2="${chartBottom}" stroke="#666" stroke-width="1" />
      <text x="0" y="${height - 2}" font-size="13" fill="#aaa" text-anchor="start">Min: ${min.toFixed(2)}</text>
      <text x="${width / 2}" y="${height - 2}" font-size="13" fill="#ee6666" text-anchor="middle">μ: ${mean.toFixed(2)} | σ: ${stdDev.toFixed(2)}</text>
      <text x="${width}" y="${height - 2}" font-size="13" fill="#aaa" text-anchor="end">Max: ${max.toFixed(2)}</text>
    </svg>
  `;
}

const labels = [];
const barSeriesData = [];
const metricsMap = {};

Object.keys(groups).forEach((stepUslKey) => {
  const groupData = groups[stepUslKey];
  const currentUsl = groupData[0].usl;
  const specAvailable = groupData[0].spec_available;

  const validV = groupData.map(d => d.v).filter(v => v !== null && !isNaN(v));
  const meanV = validV.length > 0 ? validV.reduce((sum, val) => sum + val, 0) / validV.length : 0;
  const avgStdDev = groupData.reduce((sum, d) => sum + (d.stddev_v || 0), 0) / groupData.length;

  let metricName = "";
  let metricValue = 0;

  if (specAvailable === -1 || currentUsl === null) {
    metricName = "CPK";
    metricValue = avgStdDev !== 0 ? (meanV / (3 * avgStdDev)) : 0;
  } else {
    metricName = "CPU";
    metricValue = avgStdDev !== 0 ? ((currentUsl - meanV) / (3 * avgStdDev)) : 0;
  }

  labels.push(stepUslKey);

  barSeriesData.push({
    name: stepUslKey,
    value: [1, stepUslKey]
  });

  metricsMap[stepUslKey] = {
    count: groupData.length,
    metric: metricName,
    value: metricValue.toFixed(3),
    mean: meanV.toFixed(3),
    rawValues: validV
  };
});

const bodyTextColor = getComputedStyle(document.body).color || '#fff';

return {
  grid: { top: 20, bottom: 20, left: '90%', right: 20 },
  xAxis: { type: 'value', min: 0, max: 1, show: false },
  yAxis: {
    type: 'category',
    data: labels,
    inverse: true,
    axisLine: { show: false },
    axisTick: { show: false },
    triggerEvent: true,
    axisLabel: {
      color: bodyTextColor,
      fontFamily: 'Arial',
      fontSize: 12,
      lineHeight: 18,
      margin: 15,
      formatter: function (value) {
        const m = metricsMap[value];
        if (!m) return value;
        return `${value} (n: ${m.count})\n${m.metric}: ${m.value} | Mean: ${m.mean}`;
      }
    }
  },
  tooltip: {
    trigger: 'item',
    backgroundColor: 'rgba(20, 20, 25, 0.95)',
    borderColor: '#444',
    padding: 12,
    extraCssText: 'width: 250px; max-width: 350px; box-sizing: border-box; white-space: normal;', // <--- Changed width from 200px to 300px to prevent overflow
    textStyle: { color: '#fff' },
    formatter: function (params) {
      const key = params.name || (Array.isArray(params.value) ? params.value[1] : null);
      const m = metricsMap[key];

      if (!m) return '';

      const svgHistogram = generateHistogramWithGaussianSVG(m.rawValues);

      return `
        <div style="font-family: Arial, sans-serif; font-size: 12px;">
          <strong style="color: #61afef; font-size: 13px;">${key}</strong><br/>
          <span style="color: #aaa;">Sample Size (n):</span> ${m.count}<br/>
          <span style="color: #aaa;">${m.metric}:</span> ${m.value} | <span style="color: #aaa;">Mean:</span> ${m.mean}
          <hr style="border: none; border-top: 1px solid #444; margin: 8px 0;" />
          <div style="font-weight: bold; margin-bottom: 2px;">
            Distribution & Gaussian Fit 
            <span style="color:#ee6666; font-size:11px;">(— Fitted Curve)</span>
          </div>
          ${svgHistogram}
        </div>
      `;
    }
  },
  series: [
    {
      type: 'bar',
      data: barSeriesData,
      barWidth: '90%',
      itemStyle: {
        color: 'rgba(255, 255, 255, 0.5)'
      }
    }
  ]
};
