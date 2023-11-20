SELECT animal_type, COUNT(DISTINCT "Animal ID") 
FROM outcomes 
GROUP BY animal_type;

-- How many animals are there with more than 1 outcome?
SELECT COUNT(*) 
FROM (
    SELECT "Animal ID", COUNT(*) 
    FROM outcomes 
    GROUP BY "Animal ID" 
    HAVING COUNT(*) > 1
) AS sub_query;

-- What are the top 5 months for outcomes?
SELECT EXTRACT(MONTH FROM "DateTime") AS month, COUNT(*) 
FROM outcomes 
GROUP BY month 
ORDER BY COUNT(*) DESC 
LIMIT 5;

-- Total number of kittens, adults, and seniors, with outcome "Adopted"?
SELECT 
    CASE 
        WHEN age_in_days < 365 THEN 'Kitten'
        WHEN age_in_days BETWEEN 365 AND 3650 THEN 'Adult'
        ELSE 'Senior'
    END AS cat_age_group,
    COUNT(DISTINCT "Animal ID")
FROM outcomes 
WHERE animal_type = 'Cat' AND "Outcome Type" = 'Adopted'
GROUP BY cat_age_group;


-- For each date, what is the cumulative total of outcomes up to and including this date?
SELECT "DateTime", 
       COUNT(*) OVER (ORDER BY "DateTime" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_total
FROM outcomes 
GROUP BY "DateTime"
ORDER BY "DateTime";