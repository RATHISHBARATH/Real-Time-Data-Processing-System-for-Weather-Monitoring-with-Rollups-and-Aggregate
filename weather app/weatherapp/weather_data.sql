CREATE DATABASE weather_data;

USE weather_data;

CREATE TABLE weather (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50),
    main VARCHAR(50),
    temp DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    timestamp DATETIME
);
