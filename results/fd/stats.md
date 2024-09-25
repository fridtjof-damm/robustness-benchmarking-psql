# QUERY 
```sql
 SELECT id, title, production_year FROM title WHERE kind_id = '{KIND}'
  ```

## count per *kind*
job=# SELECT COUNT(kind_id), kind_id  FROM title GROUP BY kind_id;
  count  | kind_id 
---------+---------
  662825 |       1   
   90852 |       2
  100537 |       3
  118234 |       4
   12600 |       6
 1543264 |       7
(6 rows)

## total count
job=# SELECT COUNT(kind_id) FROM title;
  count  
---------
 2528312
(1 row)

## calc selectivity of each *kind*
```sql SELECT 
    kind_id,
    COUNT(*) AS count,
    COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS selectivity
FROM 
    title
GROUP BY 
    kind_id
ORDER BY 
    kind_id;
```

## selectivities and plans used

| Scan Type | kind_id | count | selectivity |
|-----------|---------|-------|-------------|
| Bitmap Index/Heap | 1 | 662,825 | 0.2621610782 |
| Bitmap Index/Heap | 2 | 90,852 | 0.0359338563 |
| Index Scan | 3 | 100,537 | 0.0397644753 |
| Index Scan | 4 | 118,234 | 0.0467640070 |
| Index Scan | 6 | 12,600 | 0.0049835622 |
| Seq Scan | 7 | 1,543,264 | 0.6103930211 |