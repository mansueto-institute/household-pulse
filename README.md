## Eviction Defense Map

* [Project workplan](https://docs.google.com/document/d/1w9o-pM68D3nr9rKDgwtDZqzrRjwVasWdZGQk5tnHXYE/edit): 
  * Scope of work, check-in notes, and methodological notes.
* [Household Pulse Data Dictionary](https://docs.google.com/spreadsheets/d/1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo/edit#gid=974836931)
  * `question_labels` joins to PUF on `variable`
    * `description_recode` is a cleaned label for questions    
    * `universe_recode`	defines the universe that the question applies to
    * `type_of_variable` describes the type of question (ID, TIME, FLAG, GEOCODE, WEIGHT, NUMERIC, QUESTION).
  * `response_labels` joins to PUF on `variable` and `value`
    * `variable_group` groups 'select all that apply questions' (useful for subsetting question groups)
    * `variable_recode` is the new variable that uniquely identifies `label_recode`
    * `label_recode` is a cleaned label for question responses
    * `do_not_join` is a flag for variables that do not have a categorical response label
  * Question and response labels are based on Phase 3 December 9-21 dictionary and should be consistent with Week 13 onwards. Note responses that contain `-99` means "Question seen but category not selected" and `-88` means "Missing / Did not report".
  * `county_metro_state` contains a county to metro or state crosswalk
* [Household Pulse Survey Public Use Files](https://www.census.gov/programs-surveys/household-pulse-survey/datasets.html)
