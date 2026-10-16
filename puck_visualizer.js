let data = [];
context.panel.data.series.map((s) => {
  for (let i = 0; i < s.length; i++) {
    data.push({
      x: s.fields.find((f) => f.name === 'cell_X').values[i],
      y: s.fields.find((f) => f.name === 'cell_Y').values[i],
      vleft: s.fields.find((f) => f.name === 'Vleft').values[i],
      vleft_status: s.fields.find((f) => f.name === 'Vleft_status').values[i],
      vright: s.fields.find((f) => f.name === 'Vright').values[i],
      vright_status: s.fields.find((f) => f.name === 'Vright_status').values[i],
      htop: s.fields.find((f) => f.name === 'Htop').values[i],
      htop_status: s.fields.find((f) => f.name === 'Htop_status').values[i],
      hbottom: s.fields.find((f) => f.name === 'Hbottom').values[i],
      hbottom_status: s.fields.find((f) => f.name === 'Hbottom_status').values[i],
      size: 5
    })
  }
}
);
const bodyTextColor = getComputedStyle(document.body).color;
console.log(data)

let min = Infinity;
let max = -Infinity;

data.forEach((d) => {
  const values = [];
  if (d.vleft_status === 'good') values.push(d.vleft);
  if (d.vright_status === 'good') values.push(d.vright);
  if (d.htop_status === 'good') values.push(d.htop);
  if (d.hbottom_status === 'good') values.push(d.hbottom);

  if (values.length > 0) {
    min = Math.min(min, ...values);
    max = Math.max(max, ...values);
  }
});

// Fallback in case dataset is empty or values are invalid
if (min === Infinity) min = 0;
if (max === -Infinity) max = 1;

// 2. Color Mapping Function
function valueToColor(value, min, max) {
  if (min === max) return 'rgb(255, 0, 0)';

  if (value < min) value = min;
  if (value > max) value = max;

  const mid = (min + max) / 2;
  let r, g, b;

  if (value <= mid) {
    const range = mid - min;
    const t = range === 0 ? 0 : (value - min) / range;
    r = 255 * t;
    g = 255 * t;
    b = 255 * (1 - t);
  } else {
    const range = max - mid;
    const t = range === 0 ? 0 : (value - mid) / range;
    r = 255;
    g = 255 * (1 - t);
    b = 0;
  }

  return `rgb(${Math.round(r)},${Math.round(g)},${Math.round(b)})`;
}

const barsize = 10;

// 3. Map Scatter Data Series for each direction, filtering by status === 'good'
let scatterDataL = data
  .filter((d) => d.vleft_status === 'good')
  .map((d) => ({
    value: [d.x - d.size, d.y, d.vleft, d.vright, d.htop, d.hbottom],
    itemStyle: {
      color: valueToColor(d.vleft, min, max)
    }
  }));

let scatterDataR = data
  .filter((d) => d.vright_status === 'good')
  .map((d) => ({
    value: [d.x + d.size, d.y, d.vleft, d.vright, d.htop, d.hbottom],
    itemStyle: {
      color: valueToColor(d.vright, min, max)
    }
  }));

let scatterDataT = data
  .filter((d) => d.htop_status === 'good')
  .map((d) => ({
    value: [d.x, d.y + d.size, d.vleft, d.vright, d.htop, d.hbottom],
    itemStyle: {
      color: valueToColor(d.htop, min, max)
    }
  }));

let scatterDataB = data
  .filter((d) => d.hbottom_status === 'good')
  .map((d) => ({
    value: [d.x, d.y - d.size, d.vleft, d.vright, d.htop, d.hbottom],
    itemStyle: {
      color: valueToColor(d.hbottom, min, max)
    }
  }));

// 4. ECharts Configuration Output
return {
  xAxis: { show: false, min: -100, max: 100 },
  yAxis: { show: false, min: -100, max: 100 },
  grid: {
    left: 0,
    right: 0,
    top: 0,
    bottom: 0,
    containLabel: false
  },
  tooltip: {
    trigger: 'item',
    formatter: (params) => {
      // params.value array: [x, y, vleft, vright, htop, hbottom]
      return `
        Vleft: ${params.value[2]}<br/>
        Vright: ${params.value[3]}<br/>
        Htop: ${params.value[4]}<br/>
        Hbottom: ${params.value[5]}
      `;
    }
  },
  series: [
    {
      type: 'scatter',
      symbol: 'rect',
      symbolSize: barsize,
      data: scatterDataL
    },
    {
      type: 'scatter',
      symbol: 'rect',
      symbolSize: barsize,
      data: scatterDataR
    },
    {
      type: 'scatter',
      symbol: 'rect',
      symbolSize: barsize,
      data: scatterDataT
    },
    {
      type: 'scatter',
      symbol: 'rect',
      symbolSize: barsize,
      data: scatterDataB
    }
  ]
};
