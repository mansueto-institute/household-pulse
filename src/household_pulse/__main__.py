# -*- coding: utf-8 -*-
"""
Created on Saturday, 23rd October 2021 1:57:08 pm
===============================================================================
@filename:  __main__.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household pulse
@purpose:   main cli for household pulse ETLs
===============================================================================
"""
import json
import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional

import boto3
import requests
from botocore.exceptions import ClientError
from tqdm import tqdm

from household_pulse.io import Census, S3Storage
from household_pulse.preload_data.fetch_and_cache import build_front_cache
from household_pulse.pulse import Pulse
from household_pulse.smoothing import smooth_pulse

logging.basicConfig(level=logging.INFO)


class PulseCLI:
    """
    This class represents a CLI instance.
    """

    TARGETS = {"census", "s3"}

    def __init__(self, args: Optional[list[str]] = None) -> None:
        parser = ArgumentParser(
            description="Basic CLI for managing the Household Pulse ETL"
        )

        subparsers = parser.add_subparsers(
            dest="subcommand", help="The different sub-commands available"
        )

        self.etlparser = subparsers.add_parser(
            name="etl",
            help="Subcommands for managing / running the main ETL process",
        )
        self._etlparser(self.etlparser)

        self.dataparser = subparsers.add_parser(
            name="fetch",
            help=(
                "Subcommands for fetching different data from the ETL process"
            ),
        )
        self._dataparser(self.dataparser)
        self.parser = parser
        self.args = parser.parse_args(args)

    def main(self) -> None:
        """
        Main command distributor for the CLI
        """
        if self.args.subcommand == "etl":
            self.etl_subcommand()

        elif self.args.subcommand == "fetch":  # pragma: no branch
            self.fetch_subcommand()
        else:
            self.parser.print_help()  # pragma: no cover

    def download_pulse(self) -> None:
        """
        Downloads all the processed data from the S3 bucket if no week is
        passed to CLI. Otherwise, downloads the processed data for the
        specified week.
        """
        outfile = self._resolve_outpath(
            filepath=self.args.output,
            file_prefix="pulse-data",
            week=self.args.week,
        )

        s3 = S3Storage()

        if self.args.week is None:
            df = s3.download_all(file_type="processed")
        else:
            weekstr = str(self.args.week).zfill(2)
            df = s3.download_parquet(
                key=f"processed-files/pulse-{weekstr}.parquet",
            )

        df.to_csv(outfile, index=False)

    def download_raw(self) -> None:
        """
        Downloads a single week of raw data from either S3 or the Census,
        depending on what is available.
        """
        outfile = self._resolve_outpath(
            filepath=self.args.output,
            file_prefix="pulse-raw",
            week=self.args.week,
        )
        pulse = Pulse(week=self.args.week)
        pulse.download_data()
        df = pulse.df
        df.to_csv(outfile, index=False)

    def etl_subcommand(self) -> None:
        """
        Runs the `etl` subcommand.
        """
        if self.args.get_latest_week:
            week = self.get_latest_week(target=self.args.get_latest_week)
            print(
                f"Latest week available on {self.args.get_latest_week} "
                f"is {week}"
            )

        elif self.args.get_all_weeks:
            weeks = self.get_all_weeks(target=self.args.get_all_weeks)
            print(
                f"Available weeks on {self.args.get_all_weeks} are " f"{weeks}"
            )

        elif self.args.run_single_week:
            pulse = Pulse(week=self.args.run_single_week)
            pulse.process_data()
            pulse.upload_data()

        elif self.args.run_latest_week:
            pulse = Pulse(week=self.get_latest_week(target="census"))
            pulse.process_data()
            pulse.upload_data()

        elif self.args.run_multiple_weeks:
            weeks = self.args.run_multiple_weeks
            for week in tqdm(weeks, desc="Processing weeks"):
                pulse = Pulse(week=week)
                pulse.process_data()
                pulse.upload_data()

        elif self.args.run_all_weeks:
            weeks = self.get_all_weeks(target="census")
            for week in tqdm(weeks, desc="Processing weeks"):
                pulse = Pulse(week=week)
                pulse.process_data()
                pulse.upload_data()

        elif self.args.backfill:
            cenweeks = Census.get_week_year_map().keys()
            s3weeks = S3Storage().get_available_weeks(file_type="processed")
            missingweeks = set(cenweeks) - set(s3weeks)
            for week in missingweeks:
                pulse = Pulse(week=week)
                pulse.process_data()
                pulse.upload_data()

        elif self.args.run_smoothing:
            smooth_pulse()

        elif self.args.build_front_cache:
            build_front_cache()

        elif self.args.send_build_request:  # pragma: no branch
            self._build_request()
        else:
            self.etlparser.print_help()  # pragma: no cover

    def fetch_subcommand(self) -> None:
        """
        Runs the `fetch` subcommand.
        """
        if self.args.subsubcommand == "download-pulse":
            self.download_pulse()
        elif self.args.subsubcommand == "download-raw":  # pragma: no branch
            self.download_raw()

    def _etlparser(self, parser: ArgumentParser) -> None:
        """
        constructs the ETL subparser

        Args:
            parser (ArgumentParser): the subparser to edit
        """
        execgroup = parser.add_mutually_exclusive_group()

        execgroup.add_argument(
            "--get-latest-week",
            help=(
                "Returns the latest available week on the passed target. "
                'Must be one of {"s3", "census"}'
            ),
            type=str,
            metavar="TARGET",
        )
        execgroup.add_argument(
            "--get-all-weeks",
            help=(
                "Returns all available weeks on the passed target. Must be "
                'one of {"s3", "census"}'
            ),
            type=str,
            metavar="TARGET",
        )
        execgroup.add_argument(
            "--run-single-week",
            help="Runs the entire pipeline for the specified week.",
            type=int,
            metavar="WEEK",
        )
        execgroup.add_argument(
            "--run-latest-week",
            help="Runs the entire pipeline for the latest census week",
            action="store_true",
            default=False,
        )
        execgroup.add_argument(
            "--run-multiple-weeks",
            help=(
                "Runs the entire pipeline for one more more weeks passed as a "
                "space separated list of integers"
            ),
            nargs="*",
            type=int,
            default=[],
            metavar="WEEKS",
        )
        execgroup.add_argument(
            "--run-all-weeks",
            help="Runs ALL available weeks on the census",
            action="store_true",
            default=False,
        )
        execgroup.add_argument(
            "--backfill",
            help="Runs all weeks in the census that are not in the S3 bucket",
            action="store_true",
            default=False,
        )
        execgroup.add_argument(
            "--run-smoothing",
            help="Runs a LOWESS on the time series for each question",
            action="store_true",
            default=False,
        )
        execgroup.add_argument(
            "--build-front-cache",
            help=(
                "Builds a cache of all data from the S3 bucket for the front "
                "end"
            ),
            action="store_true",
            default=False,
        )
        execgroup.add_argument(
            "--send-build-request",
            help="Sends a build request to the website`s front end",
            action="store_true",
            default=False,
        )

    def _dataparser(self, parser: ArgumentParser) -> None:
        """
        constructs the data fetcher subparser

        Args:
            parser (ArgumentParser): the edited subparser
        """
        subparsers = parser.add_subparsers(
            dest="subsubcommand", help="The different sub-commands available"
        )

        dloadpulse = subparsers.add_parser(
            name="download-pulse",
            help="Subcommand for downloading the processed pulse data",
        )

        dloadpulse.add_argument(
            "--week", help="Week to download", type=int, default=None
        )

        dloadpulse.add_argument(
            "output", help="The target directory for the .csv file.", type=str
        )

        dloadraw = subparsers.add_parser(
            name="download-raw",
            help="Subcommand for downloading the raw pulse data",
        )
        dloadraw.add_argument(
            "--week", help="Week to download", type=int, required=True
        )
        dloadraw.add_argument(
            "output", help="The target directory for the .csv file", type=str
        )

    def get_latest_week(self, target: str) -> int:
        """
        Fetches the latest week available on the passet target.

        Args:
            target (str): The remote target. Must be either `census` or `s3`.

        Returns:
            int: The latest week value as an integer.
        """
        if target not in PulseCLI.TARGETS:
            self.parser.error(f"{target} must be one of {PulseCLI.TARGETS}")

        if target == "s3":
            week = max(S3Storage().get_available_weeks(file_type="processed"))
        elif target == "census":  # pragma: no branch
            week = max(Census.get_week_year_map().keys())

        return week

    def get_all_weeks(self, target: str) -> tuple[int, ...]:
        """
        Fetches all available weeks on the passed target.

        Args:
            target (str): The remote target. Must be either `census` or `s3`

        Returns:
            tuple[int]: The set of available weeks as a tuple
        """
        if target not in PulseCLI.TARGETS:
            self.parser.error(f"{target} must be one of {PulseCLI.TARGETS}")

        if target == "s3":
            weeks = tuple(
                sorted(S3Storage().get_available_weeks(file_type="processed"))
            )
        elif target == "census":  # pragma: no branch
            weeks = tuple(sorted(Census.get_week_year_map().keys()))

        return weeks

    @staticmethod
    def _resolve_outpath(
        filepath: str, file_prefix: str, week: Optional[int] = None
    ) -> Path:
        """
        Resolves an output path for saving a csv file locally

        Args:
            filepath (str): desired output path
            file_prefix (str): the prefix for the output file name
            week (Optional[int]): the week for the output file

        Returns:
            Path: resolved output path
        """
        path: Path = Path(filepath)
        path = path.resolve()

        if not path.exists():
            raise FileNotFoundError(f"directory {path} does not exist")

        if week is None:
            outfile = path.joinpath(f"{file_prefix}.csv")
        else:
            outfile = path.joinpath(f"{file_prefix}-{week}.csv")

        return outfile

    @staticmethod
    def _build_request() -> None:
        """
        Sends a build request to the vercel app
        """

        secret_name = "prod/pulse/github"
        region_name = "us-east-2"

        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager", region_name=region_name
        )
        try:
            response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            raise e

        secret = json.loads(response["SecretString"])["token"]

        logging.info("Sending build request to GitHub actions")
        url = (
            "https://api.github.com/repos/nofurtherinformation/pulse-frontend/"
            "dispatches"
        )
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": secret,
            "X-GitHub-Api-Version": "2022-11-28",
        }
        data = {"event_type": "webhook"}
        r = requests.post(url, json=data, headers=headers, timeout=5)
        print(r)


def main() -> None:
    """
    Main driver method for the CLI.
    """
    PulseCLI().main()  # pragma: no cover


if __name__ == "__main__":
    main()  # pragma: no cover
