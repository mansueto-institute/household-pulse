## Eviction Defense Map

* [Project workplan](https://docs.google.com/document/d/1w9o-pM68D3nr9rKDgwtDZqzrRjwVasWdZGQk5tnHXYE/edit): scope of work and check in notes
* [Household Pulse Data Dictionary](https://docs.google.com/spreadsheets/d/1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo/edit#gid=974836931)
  * `question_labels` join to PUF on `variable`. `description_recode` is a cleaned label for questions,	`universe_recode`	defines the universe that the question applies to, and `type_of_variable` describes the type of question (ID, TIME, BUCKETIZE, FLAG, GEOCODE, WEIGHT, QUESTION).
  * `response_labels` join to PUF on `variable` and `value`. `variable_group` groups 'select all that apply questions', `variable_recode`  `label_recode` is a cleaned label for question responses, `do_not_join` is a flag for variables that do not have a categorical response label. 
  * Question and response labels are based on Phase 3 December 9-21 dictionary and should be consistent with Week 13 onwards. 
  * `county_metro_state` contains a county to metro or state crosswalk
* [Household Pulse Survey Public Use Files](https://www.census.gov/programs-surveys/household-pulse-survey/datasets.html)
