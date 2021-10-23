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

from household_pulse.loaders import load_census_weeks
from household_pulse.mysql_wrapper import PulseSQL
from household_pulse.pulse import Pulse


def get_latest_week_census() -> int:
    """
    Gets the latest available week from the census' website

    Returns:
        int: latest week
    """
    return max(load_census_weeks())


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
        metavar='target')
    execgroup.add_argument(
        '--get-all-weeks',
        help=(
            'Returns all available weeks on the passed target. Must be one of '
            '{"rds", "census"}'),
        type=str,
        metavar='target')
    execgroup.add_argument(
        '--run-single-week',
        help='Runs the entire pipeline for the specified week.',
        type=int,
        metavar='week')

    args = parser.parse_args()

    if args.get_latest_week:
        if args.get_latest_week not in {'rds', 'census'}:
            raise ValueError(
                f'{args.get_latest_week} must be one of {{"rds", "census"}}')

        if args.get_latest_week == 'rds':
            sql = PulseSQL()
            print(f'Latest week available on RDS is {sql.get_latest_week()}')
            sql.close_connection()
        elif args.get_latest_week == 'census':
            week = get_latest_week_census()
            print(f'Latest week available on the census website is {week}')

    elif args.get_all_weeks:
        if args.get_all_weeks not in {'rds', 'census'}:
            raise ValueError(
                f'{args.get_latest_week} must be one of {{"rds", "census"}}')

        if args.get_all_weeks == 'rds':
            sql = PulseSQL()
            print(f'Weeks on RDS: {sql.get_available_weeks()}')
            sql.close_connection()
        elif args.get_all_weeks == 'census':
            print(f'Weeks on census: {tuple(sorted(load_census_weeks()))}')

    elif args.run_single_week:
        pulse = Pulse(week=args.run_single_week)
        pulse.process_data()
        pulse.upload_data()
