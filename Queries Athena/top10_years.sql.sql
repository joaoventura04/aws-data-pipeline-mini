SELECT year, mean
FROM datapipeline.weather_data
ORDER BY mean DESC
LIMIT 10;