CREATE TABLE `pulse`.`pulse` (
  `week` INT NOT NULL,
  `collection_dates` VARCHAR(255) NOT NULL,
  `xtab_var` VARCHAR(255) NOT NULL,
  `xtab_val` INT NOT NULL,
  `cbsa_title` VARCHAR(255) NULL,
  `q_var` VARCHAR(255) NOT NULL,
  `q_var_label` VARCHAR(255) NOT NULL,
  `q_val` VARCHAR(255) NOT NULL,
  `q_val_label` VARCHAR(255) NULL,
  `pweight` DOUBLE NOT NULL,
  `pweight_lower` DOUBLE NOT NULL,
  `pweight_upper` DOUBLE NOT NULL,
  `pweight_share` DOUBLE NOT NULL,
  `pweight_lower_share` DOUBLE NOT NULL,
  `pweight_upper_share` DOUBLE NOT NULL,
  `hweight` DOUBLE NOT NULL,
  `hweight_lower` DOUBLE NOT NULL,
  `hweight_upper` DOUBLE NOT NULL,
  `hweight_share` DOUBLE NOT NULL,
  `hweight_lower_share` DOUBLE NOT NULL,
  `hweight_upper_share` DOUBLE NOT NULL,
  PRIMARY KEY (`week`, `xtab_var`, `xtab_val`, `q_var`, `q_val`),
  UNIQUE INDEX `idpulsenew_UNIQUE` (`idpulsenew` ASC) VISIBLE);
