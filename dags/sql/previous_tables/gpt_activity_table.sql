SELECT DATE(Time) AS date,
       HOUR(Time) AS hour,
       ID as place_id,
       '' as name,
       '' as latitude,
       '' as longitude,
       City as city,
       Rating as rating,
       Rating_n as rating_n,
       CONCAT('[', REPLACE(TRIM(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(Popularity, '||Monday:', -1), '||', 1), 'Monday:', '')), '.', ' '), ']') AS popularity_monday,
       CONCAT('[', REPLACE(TRIM(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(Popularity, '||Tuesday:', -1), '||', 1), 'Tuesday:', '')), '.', ' '), ']') AS popularity_tuesday,
       CONCAT('[', REPLACE(TRIM(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(Popularity, '||Wednesday:', -1), '||', 1), 'Wednesday:', '')), '.', ' '), ']') AS popularity_wednesday,
       CONCAT('[', REPLACE(TRIM(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(Popularity, '||Thursday:', -1), '||', 1), 'Thursday:', '')), '.', ' '), ']') AS popularity_thursday,
       CONCAT('[', REPLACE(TRIM(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(Popularity, '||Friday:', -1), '||', 1), 'Friday:', '')), '.', ' '), ']') AS popularity_friday,
       CONCAT('[', REPLACE(TRIM(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(Popularity, '||Saturday:', -1), '||', 1), 'Saturday:', '')), '.', ' '), ']') AS popularity_saturday,
       CONCAT('[', REPLACE(TRIM(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(Popularity, '||Sunday:', -1), '||', 1), 'Sunday:', '')), '.', ' '), ']') AS popularity_sunday,
       Live as live,
       Duration as duration
FROM luxmob.gpt_activity
WHERE DATE(Time) >= '2023-01-01'
ORDER BY Time ASC;