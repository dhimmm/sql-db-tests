CREATE TABLE users (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    date_of_birth DATE,
    email VARCHAR(100)
) ENGINE=InnoDB;
