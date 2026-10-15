let all_data = [];
context.panel.data.series.forEach((s) => {
  const f_id = s.fields.find(f => f.name === 'id_block');
  const f_step = s.fields.find(f => f.name === 'd_step');
  const f_usl = s.fields.find(f => f.name === "product_upper_spec_limit");
  const f_lsl = s.fields.find(f => f.name === "product_lower_spec_limit");
  const f_spec = s.fields.find(f => f.name === "spec_available");
  const f_v = s.fields.find(f => f.name === "v");
  const f_std = s.fields.find(f => f.name === "stddev_v");

  for (let i = 0; i < s.length; i++) {
    const stepVal = f_step ? f_step.values[i] : '';
    const uslVal = f_usl ? f_usl.values[i] : null;
    const lslVal = f_lsl ? f_lsl.values[i] : null;

    all_data.push({
      id_block: f_id ? f_id.values[i] : null,
      groupKey: `${stepVal} | LSL: ${lslVal !== null ? lslVal : 'N/A'} | USL: ${uslVal !== null ? uslVal : 'N/A'}`,
      usl: uslVal,
      lsl: lslVal,
      spec_available: f_spec ? f_spec.values[i] : null,
      v: f_v ? f_v.values[i] : null,
      stddev_v: f_std ? f_std.values[i] : null,
    });
  }
});

// Group data by Step + USL + LSL
const groups = all_data.reduce((acc, item) => {
  const key = item.groupKey;
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
  const width = 270;
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
  let pathPoints = [];
  const steps = 50;

  if (stdDev > 0) {
    for (let i = 0; i <= steps; i++) {
      const xVal = min + (i / steps) * (max - min);
      const pdf = (1 / (stdDev * Math.sqrt(2 * Math.PI))) * Math.exp(-0.5 * Math.pow((xVal - mean) / stdDev, 2));
      const expectedCount = n * binWidth * pdf;

      const px = (i / steps) * width;
      const py = chartBottom - (expectedCount / maxCount) * maxBarHeight;

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

Object.keys(groups).forEach((groupKey) => {
  const groupData = groups[groupKey];
  const currentUsl = groupData[0].usl;
  const currentLsl = groupData[0].lsl;
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

  // Calculate Cp = (USL - LSL) / (6 * sigma)
  let cpValue = "N/A";
  if (
    currentUsl !== null && 
    currentLsl !== null && 
    currentUsl !== undefined && 
    currentLsl !== undefined && 
    avgStdDev > 0
  ) {
    cpValue = ((currentUsl - currentLsl) / (6 * avgStdDev)).toFixed(3);
  }

  labels.push(groupKey);

  barSeriesData.push({
    name: groupKey,
    value: [1, groupKey]
  });

  metricsMap[groupKey] = {
    count: groupData.length,
    metric: metricName,
    value: metricValue.toFixed(3),
    cp: cpValue,
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
        return `${value} (n: ${m.count})\n${m.metric}: ${m.value} | Cp: ${m.cp} | Mean: ${m.mean}`;
      }
    }
  },
  tooltip: {
    trigger: 'item',
    backgroundColor: 'rgba(20, 20, 25, 0.95)',
    borderColor: '#444',
    padding: 12,
    extraCssText: 'width: 270px; max-width: 350px; box-sizing: border-box; white-space: normal;',
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
          <span style="color: #aaa;">${m.metric}:</span> ${m.value} | <span style="color: #aaa;">Cp:</span> ${m.cp} | <span style="color: #aaa;">Mean:</span> ${m.mean}
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

// SPC pre lap post lap batch trend
let all_data = [];
const groupedData = new Map();
const ttv_threshold_pre_lap = 40;
const range_of_mean_threshold_pre_lap = 20;
const ttv_threshold_post_lap = 10;
const range_of_mean_threshold_post_lap = 4;
const bodyTextColor = getComputedStyle(document.body).color;

// 1. Parse and group your data across series using row index 'i'
context.panel.data.series.forEach((s) => {
  const f_batch = s.fields.find(f => f.name === 'Batch');
  const f_step = s.fields.find(f => f.name === 'd_step');
  const f_date = s.fields.find(f => f.name === 'date_key (max)');
  const f_step_desc = s.fields.find(f => f.name === 'd_step_description'); // Grab the new field
  const f_parameter = s.fields.find(f => f.name === 'parameter_name');
  const f_vmax = s.fields.find(f => f.name === 'v (max)');
  const f_vmin = s.fields.find(f => f.name === 'v (min)');
  const f_count = s.fields.find(f => f.name === 'id_block (distinctCount)');

  for (let i = 0; i < s.length; i++) {
    if (!groupedData.has(i)) {
      groupedData.set(i, {
        Batch: null,
        Step: null,
        date: null,
        max_of_max: null,
        min_of_min: null,
        max_of_mean: null,
        min_of_mean: null,
        count: f_count,
      });
    }
    const row = groupedData.get(i);
    if (f_batch) row.Batch = f_batch.values[i];
    if (f_date) row.date = f_date.values[i];
    if (f_count) row.count = f_count.values[i];


    // Concatenate Step name and Description safely if they exist
    if (f_step) {
      const stepName = f_step.values[i];
      const stepDesc = f_step_desc ? f_step_desc.values[i] : null;
      row.Step = stepDesc ? `${stepName} : ${stepDesc}` : stepName;
    }

    if (f_parameter) {
      const paramValue = f_parameter.values[i];
      if (paramValue === "max_thickness" && f_vmax) row.max_of_max = f_vmax.values[i];
      if (paramValue === "min_thickness" && f_vmin) row.min_of_min = f_vmin.values[i];
      if (paramValue === "mean_thickness" && f_vmax) row.max_of_mean = f_vmax.values[i];
      if (paramValue === "mean_thickness" && f_vmin) row.min_of_mean = f_vmin.values[i];
    }
  }
});

all_data = Array.from(groupedData.values());

// 2. Group by Step, and collapse identical Batches into a single row
const stepsMap = new Map();
all_data.forEach(row => {
  if (!row.Step || !row.Batch) return;

  if (!stepsMap.has(row.Step)) {
    stepsMap.set(row.Step, new Map());
  }

  const batchMap = stepsMap.get(row.Step);
  if (!batchMap.has(row.Batch)) {
    batchMap.set(row.Batch, {
      Batch: row.Batch, max_of_max: null, min_of_min: null,
      max_of_mean: null, min_of_mean: null,
      date: null,
      count: null,
    });
  }

  const finalRow = batchMap.get(row.Batch);
  if (row.date !== null) finalRow.date = row.date;
  if (row.count !== null) finalRow.count = row.count;
  if (row.max_of_max !== null) finalRow.max_of_max = row.max_of_max;
  if (row.min_of_min !== null) finalRow.min_of_min = row.min_of_min;
  if (row.max_of_mean !== null) finalRow.max_of_mean = row.max_of_mean;
  if (row.min_of_mean !== null) finalRow.min_of_mean = row.min_of_mean;
});

// 3. Extract step names dynamically
const stepNames = Array.from(stepsMap.keys());
const step1 = stepNames[0]; // pre lap
const step2 = stepNames[1]; // post lap

// Helper function to extract and sort candlestick data by date
function getStepCandleData(stepName) {
  const batchMap = stepsMap.get(stepName);
  if (!batchMap) return { batches: [], dates: [], values: [] };

  // Convert map values to array and sort chronologically by date
  const sortedBatchList = Array.from(batchMap.values()).sort((a, b) => {
    if (!a.date) return 1;
    if (!b.date) return -1;
    return new Date(a.date) - new Date(b.date);
  });

  const batches = [];
  const dates = [];
  const values = [];
  const counts = [];
  const labelData = [];

  sortedBatchList.forEach((data) => {
    batches.push(data.Batch);
    counts.push(data.count);
    dates.push(data.date || 'N/A');

    const ttv = data.max_of_max - data.min_of_min;
    const rangeOfMean = data.max_of_mean - data.min_of_mean;


    // 1. Wick Color (TTV > 40 -> Red, else Gray)
    const wickColor = ttv > ttv_threshold_pre_lap ? '#ff2f1c' : '#48ad2b';



    // 2. Body Fill Color (Range of Mean > 20 -> Yellow, else Green)
    const bodyColor = rangeOfMean > range_of_mean_threshold_pre_lap ? '#f39c12' : '#48ad2b';
    values.push({
      // ECharts Candlestick format: [Open, Close, Lowest, Highest]
      value: [
        data.min_of_mean,  // Open
        data.max_of_mean,  // Close
        data.min_of_min,   // Lowest
        data.max_of_max    // Highest
      ],
      itemStyle: {
        color: bodyColor,         // Body fill when Open <= Close
        color0: bodyColor,        // Body fill when Open > Close
        borderColor: wickColor,   // Wick line and border color
        borderColor0: wickColor,  // Wick line and border color
        borderWidth: 2            // Make the wicks and border thick and clearly visible
      }
    });

    labelData.push({
      value: [
        data.Batch,
        (Number(data.min_of_mean) + Number(data.max_of_mean)) / 2
      ],
      count: data.count
    });
  });

  return { batches, dates, values, counts, labelData };
}

const step1Data = getStepCandleData(step1);
const step2Data = getStepCandleData(step2);

// 4. Return ECharts option object
return {
  title: [
    {
      text: step1,
      left: '25%',
      top: '2%',
      textAlign: 'center',
      textStyle: { fontSize: 14, color: bodyTextColor }
    },
    {
      text: step2,
      left: '75%',
      top: '2%',
      textAlign: 'center',
      textStyle: { fontSize: 14, color: bodyTextColor }
    }
  ],
  tooltip: {
    trigger: 'axis',
    axisPointer: { type: 'cross' },
    formatter: function (params) {
      const item = params[0];
      if (!item || !item.value) return '';

      // Determine dataset according to series index
      const dataSet = item.seriesIndex === 0 ? step1Data : step2Data;
      const dateVal = dataSet.dates[item.dataIndex] || 'N/A';
      const count = dataSet.counts[item.dataIndex] ?? 'N/A';

      const [open, close, lowest, highest] = item.value.slice(1);

      const ttv = highest - lowest;
      const rangeOfMean = close - open;

      // Calculate dynamic colors matching your threshold logic
      const ttvColor = ttv > ttv_threshold_pre_lap ? '#ff2f1c' : '#48ad2b';
      const rangeColor = rangeOfMean > range_of_mean_threshold_pre_lap ? '#f39c12' : '#48ad2b';

      return `
        <strong>Batch: ${item.name}</strong><br/>
        <strong>Date:</strong> ${dateVal !== 'N/A' ? new Date(dateVal).toLocaleString() : 'N/A'}<br/>
        ------------------- ${count} pcs<br/>
        Max of Max: ${highest}<br/>
        Max of Mean: ${close}<br/>
        Min of Mean: ${open}<br/>
        Min of Min: ${lowest}<br/>
        <span style="color: ${ttvColor}; font-weight: bold;">ttv: ${Math.round(ttv)}</span><br/>
        <span style="color: ${rangeColor}; font-weight: bold;">Range of Mean: ${Math.round(rangeOfMean)}</span>
      `;
    }
  },
  grid: [
    { left: '7%', right: '53%', top: '12%', bottom: '15%' }, // Left chart (Step 1)
    { left: '55%', right: '5%', top: '12%', bottom: '15%' }  // Right chart (Step 2)
  ],
  xAxis: [
    {
      type: 'category',
      gridIndex: 0,
      data: step1Data.batches,
      name: 'Batch',
      nameLocation: 'middle',
      nameGap: 30,
      axisLabel: { rotate: 30 }
    },
    {
      type: 'category',
      gridIndex: 1,
      data: step2Data.batches,
      name: 'Batch',
      nameLocation: 'middle',
      nameGap: 30,
      axisLabel: { rotate: 30 }
    }
  ],
  yAxis: [
    {
      type: 'value',
      gridIndex: 0,
      name: 'Thickness',
      scale: true
    },
    {
      type: 'value',
      gridIndex: 1,
      name: 'Thickness',
      scale: true
    }
  ],
  series: [
    {
      name: step1,
      type: 'candlestick',
      xAxisIndex: 0,
      yAxisIndex: 0,
      data: step1Data.values
    },
    {
      name: step2,
      type: 'candlestick',
      xAxisIndex: 1,
      yAxisIndex: 1,
      data: step2Data.values
    },
    {
      name: 'Count - Step 1',
      type: 'scatter',
      xAxisIndex: 0,
      yAxisIndex: 0,
      data: step1Data.labelData,
      symbolSize: 1,
      itemStyle: {
        color: 'Black'
      },
      label: {
        show: true,
        position: 'inside',
        formatter: function (params) {
          return params.data.count ?? '';
        },
        color: 'black',
        fontSize: 10,
        fontWeight: 'bold'
      },
      tooltip: {
        show: false
      },
      z: 10
    },
    {
      name: 'Count - Step 2',
      type: 'scatter',
      xAxisIndex: 1,
      yAxisIndex: 1,
      data: step2Data.labelData,
      symbolSize: 1,
      itemStyle: {
        color: 'transparent'
      },
      label: {
        show: true,
        position: 'inside',
        formatter: function (params) {
          return params.data.count ?? '';
        },
        color: 'black',
        fontSize: 10,
        // fontWeight: 'bold'
      },
      tooltip: {
        show: false
      },
      z: 10
    }
  ]
};
