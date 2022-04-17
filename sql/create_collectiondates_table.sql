CREATE TABLE `pulse`.`collection_dates` (
    `week` INT NOT NULL,
    `pub_date` DATE NOT NULL,
    `start_date` DATE NOT NULL,
    `end_date` DATE NOT NULL,
    PRIMARY KEY (`week`, `pub_date`, `start_date`, `end_date`)
);