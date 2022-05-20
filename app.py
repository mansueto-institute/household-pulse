# -*- coding: utf-8 -*-
"""
Created on Friday, 29th October 2021 3:14:07 pm
===============================================================================
@filename:  app.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   this is the lambda function that goes into the dockerfile
===============================================================================
"""
import subprocess


def handler(event, context):
    args = event['args'].split()
    proc = subprocess.run(
        ['python3', '-m', 'household_pulse', *args],
        capture_output=True)
    cache_proc = subprocess.run(
        ['python3', '-m', 'household_pulse/fetch_and_cache.py', *args],
        capture_output=True)

    return proc.stdout