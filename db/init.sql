
--- CREATE STAGING TABLE (CATCHES ALL TRANSFORMED DATA) 
CREATE TABLE IF NOT EXISTS outcomes_staging (
    "Animal ID" VARCHAR(7),
    "Outcome" VARCHAR,
    "Date ID" INT,
    "Day" INT,
    "Month" INT,
    "Year" INT,
    "Reproductive Status" VARCHAR,
    "Name" VARCHAR,
    "Animal Type" VARCHAR,
    "Breed" VARCHAR,
    "Color" VARCHAR,
    "Sex" VARCHAR,
    "DOB" DATE);


--- CREATE DIMENSION TABLES
--- Animal Dimension
CREATE TABLE IF NOT EXISTS animal_dim (
    "Animal ID" VARCHAR(7) PRIMARY KEY,
    "Name" VARCHAR,
    "Animal Type" VARCHAR,
    "Breed" VARCHAR,
    "Color" VARCHAR,
    "Sex" VARCHAR,
    "DOB" DATE
);

--- Outcomes Dimension
CREATE TABLE IF NOT EXISTS outcome_type_dim (
    "Outcome ID" SERIAL PRIMARY KEY,
    "Outcome Type" VARCHAR UNIQUE NOT NULL
);

--- Reproductive Status Dimension
CREATE TABLE IF NOT EXISTS reproductive_status_dim (
    "Status ID" SERIAL PRIMARY KEY,
    "Reproductive Status" VARCHAR UNIQUE NOT NULL
);

--- Date Dimension
CREATE TABLE IF NOT EXISTS date_dim (
    "Date ID" INT PRIMARY KEY,
    "Day" INT,
    "Month" INT,
    "Year" INT
);


--- CREATE FACT TABLE
CREATE TABLE IF NOT EXISTS outcomes_fct (
    "Event ID" SERIAL PRIMARY KEY,
    "Date" INT,
    "Animal" VARCHAR(7),
    "Outcome" INT,
    "Repo Status" INT,
    FOREIGN KEY ("Date") REFERENCES date_dim("Date ID"),
    FOREIGN KEY ("Animal") REFERENCES animal_dim("Animal ID"),
    FOREIGN KEY ("Outcome") REFERENCES outcome_type_dim("Outcome ID"),
    FOREIGN KEY ("Repo Status") REFERENCES reproductive_status_dim("Status ID")
);