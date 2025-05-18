---
title: Attributes with multiple values WordCloud
description: Top words from the files SQLite DB
---

```sql wc
SELECT attribute as name, COUNT(DISTINCT value) AS value
FROM attribute_values
GROUP BY name
HAVING COUNT(DISTINCT value) > 1;
```

<script lang="ts">   
  /* No context="module" block, so only one scope exists */
 

  let option;             // ECharts config (reactive)

  onMount(async () => {
    // Register the word-cloud series on the client only
    await import('echarts-wordcloud');
    console.log('[wordcloud] plug-in loaded');

    option = {
      backgroundColor: 'transparent',
      tooltip: { show: true },
      series: [{
        type: 'wordCloud',
        shape: 'circle',
        gridSize: 8,
        sizeRange: [14, 60],
        rotationRange: [-90, 90],
        textStyle: {
          fontFamily: 'sans-serif',
          color() {
            const r = Math.round(Math.random() * 160);
            const g = Math.round(Math.random() * 160);
            const b = Math.round(Math.random() * 160);
            return `rgb(${r},${g},${b})`;
          },
          emphasis: { shadowBlur: 10, shadowColor: '#333' }
        },
        data: [...wc]
      }]
    };
  });
</script>

{#if option}
  <ECharts config={option} />
{/if}
