CREATE TABLE indicator_info (
    id INT PRIMARY KEY,
    countryCode VARCHAR(3),
    year INT,
    value DECIMAL(18, 2), -- Adjust the precision and scale based on your data
    rank INT,
    name VARCHAR(255),
    indicatorCode VARCHAR(255)
);