---
title: Word Prevalence
description: Show top words and list of strings from the Evidence SQLite DB
---


```sql words
SELECT
  word,
  count
FROM words.words
ORDER BY count DESC
```

<BarChart
    data={words}
    x=words
    y=count
    echartsOptions={{exampleOption: 'hello'}}
/>