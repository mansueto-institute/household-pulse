# -*- coding: utf-8 -*-
"""
Created on Friday, 22nd April 2022 5:09:03 pm
===============================================================================
@filename:  smoothing.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   this module contains the functionality that takes the raw
            processed timeseries and creates a separate table with smoothed
            values across time.
===============================================================================
"""

import pandas as pd
import statsmodels.api as sm

from household_pulse.mysql_wrapper import PulseSQL


def smooth_group(group: pd.DataFrame, frac: float = 0.2) -> pd.DataFrame:
    wcols = [
        'pweight_share',
        'pweight_lower_share',
        'pweight_upper_share',
        'hweight_share',
        'hweight_lower_share',
        'hweight_upper_share'
    ]
    for wcol in wcols:
        smoothed = sm.nonparametric.lowess(
            exog=group['week'],
            endog=group[wcol],
            frac=frac)
        group[f'{wcol}_smoothed'] = smoothed[:, 1]
        group.drop(columns=wcol, inplace=True)

    return group


if __name__ == "__main__":
    sql = PulseSQL()

    query = '''
        SELECT week,
            xtab_var,
            xtab_val,
            q_var,
            q_val,
            pweight_share,
            pweight_lower_share,
            pweight_upper_share,
            hweight_share,
            hweight_lower_share,
            hweight_upper_share
        FROM pulse
    '''

    df = sql.get_pulse_table(query=query)

    df = df.groupby(['xtab_var', 'xtab_val', 'q_var', 'q_val']).apply(
        smooth_group)
    df.sort_values(
        by=['week', 'xtab_var', 'xtab_val', 'q_var', 'q_val'],
        inplace=True)
    sql.update_values(table='smoothed', df=df)
    sql.close_connection()
