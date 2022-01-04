CREATE TABLE `pulse`.`response_mapping` (
  `variable` VARCHAR(255) NULL,
  `value` VARCHAR(255) NULL,
  `label` VARCHAR(255) NULL,
  `value_recode` INT NULL,
  `label_recode` VARCHAR(255) NULL,
  `drop_no` INT NULL,
  `value_binary` INT NULL,
  `label_binary` VARCHAR(255) NULL,
  `binary_flag` INT NULL,
  `do_not_join` INT NULL)
ENGINE = InnoDB;
