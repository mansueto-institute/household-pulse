# Household Pulse Survey

Automates the processing of Census Household Pulse biweekly data into a crosstabs

## External links

* [Project workplan](https://docs.google.com/document/d/1w9o-pM68D3nr9rKDgwtDZqzrRjwVasWdZGQk5tnHXYE/edit):
  * Scope of work, check-in notes, and methodological notes.
* [Household Pulse Survey Public Use Files](https://www.census.gov/programs-surveys/household-pulse-survey/datasets.html)
* [Household Pulse Data Dictionary](https://docs.google.com/spreadsheets/d/1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo/edit#gid=974836931)
  * `question_labels` joins to PUF on `variable`
    * `description_recode` is a cleaned label for questions
    * `universe_recode` defines the universe that t
    he question applies to
    * `type_of_variable` describes the type of question (ID, TIME, FLAG, GEOCODE, WEIGHT, NUMERIC, QUESTION).
  * `response_labels` joins to PUF on `variable` and `value`
    * `variable_group` groups 'select all that apply questions' (useful for subsetting question groups)
    * `variable_recode` is the new variable that uniquely identifies `label_recode`
    * `label_recode` is a cleaned label for question responses
    * `do_not_join` is a flag for variables that do not have a categorical response label
  * Question and response labels are based on Phase 3 December 9-21 dictionary and should be consistent with Week 13 onwards. Note responses that contain `-99` means "Question seen but category not selected" and `-88` means "Missing / Did not report".
  * `county_metro_state` contains a county to metro or state crosswalk

## Run workflow locally

### Setup

#### 1. Install the `household_pulse` package

You can do this two ways. You can either clone the repo and install the project locally, or you can install directly from GitHub.

To clone and install:

```bash
git clone https://github.com/mansueto-institute/household-pulse
pip install -e household-pulse/
```

If you would like to install directly:

``` bash
pip install git+https://github.com/mansueto-institute/household-pulse
```

In order to upload the results to our database you will need the RDS credentials; ask your supervisor for them.

### Run

The `household_pulse` package has a CLI that you can access like any other CLI package in Python. In order to see what you can do via the CLI, you can type:

``` bash
python -m household_pulse --help
```

#### Subcommands

##### ETL

The main ETL actions are grouped under a CLI subcommand. You can read more about what features this has by running:

```bash
python -m household_pulse etl --help
```

##### Downloading Data

Another of the features that the CLI has is the ability to download the processed data to a local file in case you need to work on it locally. The best idea would be to fetch the data directly from our SQL database, but this is not always possible. You can explore which datasets you can download by running:

```bash
python -m household_pulse fetch --help
```

### Updating vignette

Clone and setup environment:
```bash
git clone https://github.com/mansueto-institute/household-pulse
cd ./household-pulse   
conda create --name pulse python=3.9.7 --yes   
source activate pulse 
pip install git+https://github.com/mansueto-institute/household-pulse
```

Add the `s3.json` and `rds-mysql.json` credentials to the repository's folder `./household-pulse/household_pulse` (link to credentials [here](https://drive.google.com/drive/folders/1f1N6_LbMW454YmHWf6QZ7PDoPSNeGUix?usp=sharing)).

Update the time series, smooth the estimates, and send build request to front end:
```bash
python -m household_pulse etl --backfill  
python -m household_pulse etl --run-smoothing 
python -m household_pulse etl --build-front-cache
python -m household_pulse etl --send-build-request 
```

## Run workflow on AWS

The workflow can be run on AWS in many different ways, depending on the need. We chose to create a Docker file that can be uploaded to ECR and then mounted as a Lambda function on AWS. The lambda function can be triggered remotely via an API, or can be scheduled to be triggered via CloudWatch as an event.

### Updating the Docker file

In order to push updates do ECR, you can do so via the AWS CLI following [this](https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-cli.html) guide.

### Updating the Lambda function

In order to deploy the Docker image into the Lambda service, or simply to push an updated Docker image into Lambda, you can follow [this](https://docs.aws.amazon.com/lambda/latest/dg/configuration-images.html) guide.
