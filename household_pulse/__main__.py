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
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from household_pulse.downloader import DataLoader
from household_pulse.loaders import load_census_weeks
from household_pulse.mysql_wrapper import PulseSQL
from household_pulse.pulse import Pulse


class PulseCLI:
    def __init__(self) -> None:
        parser = ArgumentParser(
            description='Basic CLI for managing the Household Pulse ETL'
        )

        subparsers = parser.add_subparsers(
            dest='subcommand',
            help='The different sub-commands available'
        )

        etlparser = subparsers.add_parser(
            name='etl',
            help='Subcommands for managing / running the main ETL process'
        )
        self._etlparser(etlparser)

        dataparser = subparsers.add_parser(
            name='fetch',
            help='Subcommands for fetching different data from the ETL process'
        )
        self._dataparser(dataparser)

        self.args = parser.parse_args()

    def main(self) -> None:
        """
        Main command distributor for the CLI
        """
        if self.args.subcommand == 'etl':
            if self.args.get_latest_week:
                week = self.get_latest_week(target=self.args.get_latest_week)
                print(
                    f'Latest week available on {self.args.get_latest_week} '
                    f'is {week}')

            elif self.args.get_all_weeks:
                weeks = self.get_all_weeks(target=self.args.get_all_weeks)
                print(
                    f'Available weeks on {self.args.get_all_weeks} are '
                    f'{weeks}')

            elif self.args.run_single_week:
                pulse = Pulse(week=self.args.run_single_week)
                pulse.process_data()
                pulse.upload_data()

            elif self.args.run_latest_week:
                pulse = Pulse(week=self.get_latest_week(target='census'))
                pulse.process_data()
                pulse.upload_data()

            elif self.args.run_multiple_weeks:
                weeks = self.args.run_multiple_weeks
                for week in tqdm(weeks, desc='Processing weeks'):
                    pulse = Pulse(week=week)
                    pulse.process_data()
                    pulse.upload_data()

            elif self.args.backfill:
                cenweeks = load_census_weeks()

                sql = PulseSQL()
                rdsweeks = sql.get_available_weeks()
                sql.close_connection()

                missingweeks = set(cenweeks) - set(rdsweeks)
                for week in missingweeks:
                    pulse = Pulse(week=week)
                    pulse.process_data()
                    pulse.upload_data()

            elif self.args.update_gsheet:
                target = self.args.update_gsheet
                self.update_gsheet(target=target)
        elif self.args.subcommand == 'fetch':
            if self.args.subsubcommand == 'download-pulse':
                self.download_pulse()
            elif self.args.subsubcommand == 'download-raw':
                self.download_raw()

    def download_pulse(self) -> None:
        """
        downloads the entire processed data form our RDS DB.
        """
        outfile = self._resolve_outpath(
            filepath=self.args.output,
            file_prefix='pulse-data',
            week=self.args.week
        )

        sql = PulseSQL()

        if self.args.week is None:
            df = sql.get_pulse_table()
        else:
            query = f'SELECT * FROM pulse.pulse WHERE week = {self.args.week}'
            df = sql.get_pulse_table(query)

        df.to_csv(outfile, index=False)
        sql.close_connection()

    def download_raw(self) -> None:
        """
        Downloads a single week of raw data from either S3 or the Census,
        depending on what is available.
        """
        outfile = self._resolve_outpath(
            filepath=self.args.output,
            file_prefix='pulse-raw',
            week=self.args.week
        )
        dl = DataLoader()
        df = dl.load_week(week=self.args.week)
        df.to_csv(outfile, index=False)

    def _etlparser(self, parser: ArgumentParser) -> None:
        """
        constructs the ETL subparser

        Args:
            parser (ArgumentParser): the subparser to edit
        """
        execgroup = parser.add_mutually_exclusive_group()

        execgroup.add_argument(
            '--get-latest-week',
            help=(
                'Returns the latest available week on the passed target. '
                'Must be one of {"rds", "census"}'),
            type=str,
            metavar='TARGET')
        execgroup.add_argument(
            '--get-all-weeks',
            help=(
                'Returns all available weeks on the passed target. Must be '
                'one of {"rds", "census"}'),
            type=str,
            metavar='TARGET')
        execgroup.add_argument(
            '--run-single-week',
            help='Runs the entire pipeline for the specified week.',
            type=int,
            metavar='WEEK')
        execgroup.add_argument(
            '--run-latest-week',
            help='Runs the entire pipeline for the latest census week',
            action='store_true',
            default=False)
        execgroup.add_argument(
            '--run-multiple-weeks',
            help=(
                'Runs the entire pipeline for one more more weeks passed as a '
                'space separated list of integers'),
            nargs='*',
            type=int,
            default=[],
            metavar='WEEKS')
        execgroup.add_argument(
            '--backfill',
            help='Runs all weeks in the census that are not in the RDS DB',
            action='store_true',
            default=False
        )
        execgroup.add_argument(
            '--update-gsheet',
            help='uploads a google sheets table to the SQL DB',
            type=str,
            metavar='GSHEET TABLE NAME'
        )

    def _dataparser(self, parser: ArgumentParser) -> None:
        """
        constructs the data fetcher subparser

        Args:
            parser (ArgumentParser): the edited subparser
        """
        subparsers = parser.add_subparsers(
            dest='subsubcommand',
            help='The different sub-commands available'
        )

        dloadpulse = subparsers.add_parser(
            name='download-pulse',
            help='Subcommand for downloading the processed pulse data'
        )

        dloadpulse.add_argument(
            '--week',
            help='Week to download',
            type=int,
            default=None
        )

        dloadpulse.add_argument(
            'output',
            help='The target directory for the .csv file.',
            type=str
        )

        dloadraw = subparsers.add_parser(
            name='download-raw',
            help='Subcommand for downloading the raw pulse data'
        )
        dloadraw.add_argument(
            '--week',
            help='Week to download',
            type=int,
            required=True
        )
        dloadraw.add_argument(
            'output',
            help='The target directory for the .csv file',
            type=str
        )

    @staticmethod
    def get_latest_week(target: str) -> int:
        """
        Fetches the latest week available on the passet target.

        Args:
            target (str): The remote target. Must be either `census` or `rds`.

        Returns:
            int: The latest week value as an integer.
        """
        if target not in {'rds', 'census'}:
            raise ValueError(f'{target} must be one of {{"rds", "census"}}')

        if target == 'rds':
            sql = PulseSQL()
            week = sql.get_latest_week()
            sql.close_connection()
        elif target == 'census':
            week = max(load_census_weeks())

        return week

    @staticmethod
    def get_all_weeks(target: str) -> tuple[int, ...]:
        """
        Fetches all available weeks on the passed target.

        Args:
            target (str): The remote target. Must be either `census` or `rds`

        Returns:
            tuple[int]: The set of available weeks as a tuple
        """
        if target not in {'rds', 'census'}:
            raise ValueError(f'{target} must be one of {{"rds", "census"}}')

        if target == 'rds':
            sql = PulseSQL()
            weeks = sql.get_available_weeks()
            sql.close_connection()
        elif target == 'census':
            weeks = tuple(sorted(load_census_weeks()))

        return weeks

    @staticmethod
    def update_gsheet(target: str) -> None:
        """
        pushes one of the tables in google sheets to the MySQL DB

        Args:
            target (str): {'question_mapping', 'response_mapping'}
        """
        allowed_targets = {'question_mapping', 'response_mapping'}
        if target not in allowed_targets:
            raise ValueError(
                f'{target} is not in allowed targets: {allowed_targets}')

        dl = DataLoader()
        df = dl.load_gsheet(target)

        if target == 'response_mapping':
            df['value_recode'] = df['value_recode'].astype('Int32')
            df['value_binary'] = df['value_binary'].astype('Int32')
            df = df.astype('object')

        sql = PulseSQL()
        sql.cur.execute(f'DELETE FROM {target}')
        sql.append_values(table=target, df=df)
        sql.close_connection()

    @staticmethod
    def _resolve_outpath(filepath: str,
                         file_prefix: str,
                         week: Optional[int] = None) -> Path:
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
            raise FileNotFoundError(f'directory {path} does not exist')

        if week is None:
            outfile = path.joinpath(f'{file_prefix}.csv')
        else:
            outfile = path.joinpath(f'{file_prefix}-{week}.csv')

        return outfile


if __name__ == "__main__":
    PulseCLI().main()
