# Housholed Pulse Survey

Automates the processing of Census Household Pulse biweekly data into a crosstabs 

## External links
* [Project workplan](https://docs.google.com/document/d/1w9o-pM68D3nr9rKDgwtDZqzrRjwVasWdZGQk5tnHXYE/edit): 
  * Scope of work, check-in notes, and methodological notes.
* [Household Pulse Survey Public Use Files](https://www.census.gov/programs-surveys/household-pulse-survey/datasets.html)
* [Household Pulse Data Dictionary](https://docs.google.com/spreadsheets/d/1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo/edit#gid=974836931)
  * `question_labels` joins to PUF on `variable`
    * `description_recode` is a cleaned label for questions    
    * `universe_recode`	defines the universe that t
    he question applies to
    * `type_of_variable` describes the type of question (ID, TIME, FLAG, GEOCODE, WEIGHT, NUMERIC, QUESTION).
  * `response_labels` joins to PUF on `variable` and `value`
    * `variable_group` groups 'select all that apply questions' (useful for subsetting question groups)
    * `variable_recode` is the new variable that uniquely identifies `label_recode`
    * `label_recode` is a cleaned label for question responses
    * `do_not_join` is a flag for variables that do not have a categorical response label
  * Question and response labels are based on Phase 3 December 9-21 dictionary and should be consistent with Week 13 onwards. Note responses that contain `-99` means "Question seen but category not selected" and `-88` means "Missing / Did not report".
  * `county_metro_state` contains a county to metro or state crosswalk
* [Cloud Drive](https://drive.google.com/drive/u/0/folders/14LK-dEay1G9UpBjXw6Kt9eTXwZjx8rj9)
* [Databricks project](https://drive.google.com/drive/u/0/folders/14LK-dEay1G9UpBjXw6Kt9eTXwZjx8rj9)


## Run workflow locally 

### Setup

#### 1. Create a virtual environment and install packages from `requirements.txt`: 

(Run at the root of the project directory)

```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt 
```
#### 2. Setup Google Cloud Project authentication:

To run the workflow locally you need to create a GCP authentication key that you store locally. 

First, download service account key: 
  - Navigate [here](https://console.cloud.google.com/iam-admin/serviceaccounts/details/108375930580289490888;edit=true?previousPage=%2Fapis%2Fcredentials%3Fauthuser%3D1%26project%3Dhousehold-pulse&authuser=1&folder=&organizationId=&project=household-pulse)
  - Click on the `KEYS` tab along the top
  - Click the `ADD KEY` dropdown, and select `Create new key`
  - Select the `JSON (recommended)` option, and it should download a JSON file
  - Save this file somewhere secure on your local machine

Save the path to the file as an environmental variable:
- Add the following line to your `.bash_profile` or `bashrc` file:

  ```bash
  export GOOGLE_APPLICATION_CREDENTIALS="<path to JSON key on your machine>"
  ```
- Start a new terminal and check it worked by running the following:

```bash
$GOOGLE_APPLICATION_CREDENTIALS
```
You should see the path to the JSON file returned 

### Run

Make sure your virtual environment is activated, and then from the root of the repository run:

```bash
python3 prod/generate_crosstabs.py LOCAL
```

N.B. the `LOCAL` argument is required to make the workflow run locally. 

For local development can also change the `LOCAL` parameter manually [here](https://github.com/mansueto-institute/household-pulse/blob/main/prod/generate_crosstabs.py#L311)

## Run workflow in DataBricks
