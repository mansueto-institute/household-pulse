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
import mysql.connector
import pandas as pd
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.errors import DatabaseError

from household_pulse.loaders import load_rds_creds


class PulseSQL:
    def __init__(self) -> None:
        self._establish_connection()

    def _establish_connection(self) -> None:
        """
        starts connection to the DB. gets called automatically when the class
        is instantiated, ability to re run it to reconnect if needed.
        """
        self.con: MySQLConnection = mysql.connector.connect(**load_rds_creds())
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

    def _delete_week(self, week: int) -> None:
        """
        deletes all records that match the passed week value

        Args:
            table (str): [description]
            df (pd.DataFrame): [description]
        """
        query = '''
            DELETE FROM pulse
            WHERE WEEK = %s
        '''
        try:
            self.cur.execute(query, (week, ))
            self.con.commit()
        except DatabaseError as error:
            self.con.rollback()
            self.close_connection()
            raise error

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
        deletes old values from the passed `df` WEEK and inserts the new
        values passed in `df`

        Args:
            table (str): table to update in
            df (pd.DataFrame): data to use for updating
        Raises:
            DatabaseError: any issues with the DB connection
        """
        if df['WEEK'].nunique() != 1:
            raise ValueError(
                'the number of unique values for WEEK in `df` must be unique')
        self._delete_week(week=int(df['WEEK'].min()))
        self.append_values(table=table, df=df)

    def get_latest_week(self) -> int:
        """
        Gets latest week available in RDS.

        Returns:
            int: latest week loaded into RDS
        """
        self.cur.execute('SELECT MAX(WEEK) FROM pulse;')
        result = int(self.cur.fetchone()[0])

        return result

    def get_available_weeks(self) -> tuple[int, ...]:
        self.cur.execute('SELECT DISTINCT(WEEK) FROM pulse ORDER BY WEEK')
        result = tuple(int(x[0]) for x in self.cur.fetchall())

        return result

    def close_connection(self) -> None:
        """
        Closes the connection to the DB
        """
        self.con.close()
