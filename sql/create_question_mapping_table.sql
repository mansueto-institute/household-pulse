CREATE TABLE `pulse`.`question_mapping` (
  `variable` VARCHAR(45) NOT NULL,
  `variable_group` VARCHAR(45) NOT NULL,
  `variable_order` INT NOT NULL,
  `question_type` VARCHAR(45) NOT NULL,
  `question` VARCHAR(1000) NOT NULL,
  `question_clean` VARCHAR(1000) NOT NULL,
  `description_recode` VARCHAR(1000) NOT NULL,
  `universe` VARCHAR(1000) NOT NULL,
  `xtab` INT NOT NULL,
  `subtopic_area_order` INT NOT NULL,
  `subtopic_area` VARCHAR(45) NOT NULL,
  `topic_area_order` INT NOT NULL,
  `topic_area` VARCHAR(45) NOT NULL
)