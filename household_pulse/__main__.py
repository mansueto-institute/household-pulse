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
import argparse

from tqdm import tqdm

from household_pulse.loaders import load_census_weeks, load_gsheet
from household_pulse.mysql_wrapper import PulseSQL
from household_pulse.pulse import Pulse


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

    df = load_gsheet(target)

    if target == 'response_mapping':
        df['value_recode'] = df['value_recode'].astype('Int32')
        df['value_binary'] = df['value_binary'].astype('Int32')
        df = df.astype('object')

    sql = PulseSQL()
    sql.cur.execute(f'DELETE FROM {target}')
    sql.append_values(table=target, df=df)
    sql.close_connection()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Basic CLI for managing the Household Pulse ETL'
    )

    execgroup = parser.add_mutually_exclusive_group()

    execgroup.add_argument(
        '--get-latest-week',
        help=(
            'Returns the latest available week on the passed target. Must be '
            'one of {"rds", "census"}'),
        type=str,
        metavar='TARGET')
    execgroup.add_argument(
        '--get-all-weeks',
        help=(
            'Returns all available weeks on the passed target. Must be one of '
            '{"rds", "census"}'),
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

    args = parser.parse_args()

    if args.get_latest_week:
        week = get_latest_week(target=args.get_latest_week)
        print(f'Latest week available on {args.get_latest_week} is {week}')

    elif args.get_all_weeks:
        weeks = get_all_weeks(target=args.get_all_weeks)
        print(f'Available weeks on {args.get_all_weeks} are {weeks}')

    elif args.run_single_week:
        pulse = Pulse(week=args.run_single_week)
        pulse.process_data()
        pulse.upload_data()

    elif args.run_latest_week:
        pulse = Pulse(week=get_latest_week(target='census'))
        pulse.process_data()
        pulse.upload_data()

    elif args.run_multiple_weeks:
        weeks = args.run_multiple_weeks
        for week in tqdm(weeks, desc='Processing weeks'):
            pulse = Pulse(week=week)
            pulse.process_data()
            pulse.upload_data()

    elif args.backfill:
        cenweeks = load_census_weeks()

        sql = PulseSQL()
        rdsweeks = sql.get_available_weeks()
        sql.close_connection()

        missingweeks = set(cenweeks) - set(rdsweeks)
        for week in missingweeks:
            pulse = Pulse(week=week)
            pulse.process_data()
            pulse.upload_data()

    elif args.update_gsheet:
        target = args.update_gsheet
        update_gsheet(target=target)
