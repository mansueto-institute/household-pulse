# -*- coding: utf-8 -*-
"""
Created on Saturday, 16th October 2021 1:35:51 pm
===============================================================================
@filename:  mysql_wrapper.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   mysql wrapper that extends functionality for pushing data
            into the project table
===============================================================================
"""
import json
import warnings
from typing import Optional

import mysql.connector
import pandas as pd
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.errors import DatabaseError
from pkg_resources import resource_filename

from household_pulse.downloader import DataLoader


class PulseSQL:
    def __init__(self) -> None:
        self._establish_connection()

    def append_values(self, table: str, df: pd.DataFrame) -> None:
        """
        appends an entire dataframe to an existing table

        Args:
            table (str): table name
            df (pd.DataFrame): data to append
        """
        cols = ', '.join(df.columns.tolist())
        vals = ', '.join(['%s'] * len(df.columns))
        query = f'''
            INSERT INTO {table} ({cols})
            VALUES ({vals})
        '''
        df = self._convert_nans(df=df)
        try:
            self.cur.executemany(query, df.values.tolist())
            self.con.commit()
        except DatabaseError as error:
            self.con.rollback()
            self.con.close()
            raise error

    def update_values(self,
                      table: str,
                      df: pd.DataFrame) -> None:
        """
        deletes old values from the passed `df` week and inserts the new
        values passed in `df`

        Args:
            table (str): table to update in
            df (pd.DataFrame): data to use for updating
        Raises:
            DatabaseError: any issues with the DB connection
        """
        if df['week'].nunique() != 1:
            raise ValueError(
                'the number of unique values for week in `df` must be unique')
        try:
            self._delete_week(week=int(df['week'].min()), commit=False)
            self.append_values(table=table, df=df)
        except DatabaseError as e:
            self.con.rollback()
            self.close_connection()
            raise e

    def get_latest_week(self) -> int:
        """
        Gets latest week available in RDS.

        Returns:
            int: latest week loaded into RDS
        """
        self.cur.execute('SELECT MAX(week) FROM pulse;')
        result = int(self.cur.fetchone()[0])

        return result

    def get_available_weeks(self) -> tuple[int, ...]:
        self.cur.execute('SELECT DISTINCT(week) FROM pulse ORDER BY week')
        result = tuple(int(x[0]) for x in self.cur.fetchall())

        return result

    def close_connection(self) -> None:
        """
        Closes the connection to the DB
        """
        self.con.close()

    def get_pulse_table(self, query: Optional[str] = None) -> pd.DataFrame:
        """
        gets the entire pulse database with the timeseries of the survey
        responses.

        Args:
            query (str, optional): The SQL query to run against the pulse
                table. Defaults to None, and it gets the entire table by
                default.

        Returns:
            pd.DataFrame: the entire table as a dataframe object
        """
        try:
            if query is None:
                query = '''
                    SELECT * FROM pulse.pulse;
                '''

            with warnings.catch_warnings():
                # we ignore the warning that pandas gives us for not using
                # sql alchemy
                warnings.simplefilter('ignore')
                df = pd.read_sql(sql=query, con=self.con)
        except DatabaseError as e:
            self.con.rollback()
            self.close_connection()
            raise DatabaseError(e)

        return df

    def upload_collection_dates(self) -> None:
        """
        truncates the current `collection_dates` table and uploads the latest
        collection dates into the RDS database.
        """
        dl = DataLoader()
        collection_dates = dl.load_collection_dates()
        self.cur.execute('TRUNCATE pulse.collection_dates')

        datdf = pd.DataFrame.from_dict(collection_dates, orient='index')
        datdf.reset_index(inplace=True)
        datdf.rename(columns={'index': 'week'}, inplace=True)
        self.append_values(table='collection_dates', df=datdf)

    def _establish_connection(self) -> None:
        """
        starts connection to the DB. gets called automatically when the class
        is instantiated, ability to re run it to reconnect if needed.
        """
        self.con: MySQLConnection = mysql.connector.connect(
            **self._load_rds_creds())
        self.cur: MySQLCursor = self.con.cursor()

    def _convert_nans(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        converts all nan values to `None`, which are understood by the SQL
        engine as NULL

        Args:
            df (pd.DataFrame): data with nans

        Returns:
            pd.DataFrame: data with nans as Nones
        """
        return df.where(df.notnull(), None)

    def _delete_week(self, week: int, commit: bool = True) -> None:
        """
        deletes all records that match the passed week value

        Args:
            week (int): the week to remove from the pulse table
            commit (bool): whether to commit the deletion or delay it
        """
        query = '''
            DELETE FROM pulse
            WHERE week = %s
        '''
        try:
            self.cur.execute(query, (week, ))
            if commit:
                self.con.commit()
        except DatabaseError as error:
            self.con.rollback()
            self.close_connection()
            raise error

    @staticmethod
    def _load_rds_creds() -> dict[str, str]:
        """
        Loads credentials for RDS MySQL DB from local secrets file

        Returns:
            dict[str, str]: connection config dict
        """
        fname = resource_filename('household_pulse', 'rds-mysql.json')
        with open(fname, 'r') as file:
            return json.loads(file.read())
