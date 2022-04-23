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
import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.api as sm
from tqdm import tqdm

from household_pulse.mysql_wrapper import PulseSQL

if __name__ == "__main__":
    sql = PulseSQL()

    query = '''
        SELECT *
        FROM pulse
    '''
    df = sql.get_pulse_table(query=query)
    print('done download')

    frac = 0.20
    grouped = (
        df
        .groupby(['xtab_var', 'xtab_val', 'q_var', 'q_val'])
        .apply(lambda x: sm.nonparametric.lowess(
            exog=x['week'],
            endog=x['pweight_share'],
            frac=0.2))
    )
    cols = ['xtab_var', 'xtab_val', 'q_var', 'q_val']
    auxdfs = []
    for row in tqdm(grouped.iteritems()):
        index, values = row
        auxdf = pd.DataFrame(
            values,
            columns=('week', 'pweight_share_smoothed'))

        for col, val in zip(cols, index):
            auxdf[col] = val
        auxdfs.append(auxdf)

    test = pd.concat(auxdfs)
    df = df.merge(test, on=['week', 'xtab_var', 'xtab_val', 'q_var', 'q_val'])
