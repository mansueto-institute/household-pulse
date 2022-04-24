CREATE TABLE `pulse`.`smoothed` (
    `week` INT NOT NULL,
    `xtab_var` VARCHAR(255) NOT NULL,
    `xtab_val` INT NOT NULL,
    `q_var` VARCHAR(255) NOT NULL,
    `q_val` VARCHAR(255) NOT NULL,
    `pweight_share_smoothed` DOUBLE NOT NULL,
    `pweight_lower_share_smoothed` DOUBLE NOT NULL,
    `pweight_upper_share_smoothed` DOUBLE NOT NULL,
    `hweight_share_smoothed` DOUBLE NOT NULL,
    `hweight_lower_share_smoothed` DOUBLE NOT NULL,
    `hweight_upper_share_smoothed` DOUBLE NOT NULL,
    PRIMARY KEY (`week`, `xtab_var`, `xtab_val`, `q_var`, `q_val`));