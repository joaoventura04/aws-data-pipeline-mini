SELECT 
  CASE 
    WHEN year < 1900 THEN '1800s'
    WHEN year < 2000 THEN '1900s'
    ELSE '2000s'
  END AS century,
  AVG(mean) AS avg_temp
FROM datapipeline.weather_data
GROUP BY 1
ORDER BY 1;