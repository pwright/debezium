---
title: Files
description: Show file attributes distribution
---


```sql attributes
SELECT attribute as name, COUNT(DISTINCT path) AS value
FROM file_attributes
GROUP BY attribute
```
