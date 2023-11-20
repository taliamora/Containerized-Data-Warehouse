--- INSERT DATA INTO DIMENSION TABLES

--- Animal Dimension
INSERT INTO animal_dim ("Animal ID", "Name", "Animal Type", "Breed", "Color", "Sex", "DOB")
SELECT DISTINCT "Animal ID", "Name", "Animal Type", "Breed", "Color", "Sex", "DOB"
FROM outcomes_staging
WHERE "Animal ID" IS NOT NULL;

--- Outcomes Dimension
INSERT INTO outcome_type_dim ("Outcome Type")
SELECT DISTINCT "Outcome"
FROM outcomes_staging
WHERE "Outcome" IS NOT NULL;

--- Reproductive Status Dimension
INSERT INTO reproductive_status_dim ("Reproductive Status")
SELECT DISTINCT "Reproductive Status"
FROM outcomes_staging
WHERE "Reproductive Status" IS NOT NULL;

--- Date Dimension
INSERT INTO date_dim ("Date ID", "Day", "Month", "Year")
SELECT DISTINCT "Date ID", "Day", "Month", "Year"
FROM outcomes_staging
WHERE "Date ID" IS NOT NULL;




--- INSERT DATA INTO FACT TABLE
INSERT INTO outcomes_fct ("Date", "Animal", "Outcome", "Repo Status")
SELECT 
    dd."Date ID",
    ad."Animal ID",
    otd."Outcome ID",
    rsd."Status ID"
FROM outcomes_staging os
JOIN date_dim dd ON os."Date ID" = dd."Date ID"
JOIN animal_dim ad ON os."Animal ID" = ad."Animal ID"
JOIN outcome_type_dim otd ON os."Outcome" = otd."Outcome Type"
JOIN reproductive_status_dim rsd ON os."Reproductive Status" = rsd."Reproductive Status";

