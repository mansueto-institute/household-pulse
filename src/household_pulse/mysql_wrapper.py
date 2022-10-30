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
from datetime import datetime
from typing import Optional
import logging

import boto3
import mysql.connector
import pandas as pd
from botocore.exceptions import ClientError
from mysql.connector import Error, MySQLConnection
from mysql.connector.cursor import MySQLCursor

from household_pulse.downloader import DataLoader

logger = logging.getLogger(__name__)


class PulseSQL:
    """
    This class represents a connection to the RDS database where we store
    the processed data results.
    """

    def __init__(self) -> None:
        self._establish_connection()

    def append_values(
        self, table: str, df: pd.DataFrame, commit: bool = True
    ) -> None:
        """
        appends an entire dataframe to an existing table

        Args:
            table (str): table name
            df (pd.DataFrame): data to append
            commit (bool): if to commit the transaction after the insert
        """
        self._refresh_connection()
        cols = ", ".join(df.columns.tolist())
        vals = ", ".join(["%s"] * len(df.columns))
        query = f"""
            INSERT INTO {table} ({cols})
            VALUES ({vals})
        """
        df = self._convert_nans(df=df)
        try:
            c: MySQLCursor = self.conn.cursor()
            logger.info("Inserting %s records into table %s", len(df), table)
            c.executemany(query, df.values.tolist())
            if commit:
                logger.info("Committing transaction to RDS")
                self.conn.commit()
            c.close()
        except Error as error:
            self.conn.rollback()
            c.close()
            self.close_connection()
            raise error

    def update_values(self, table: str, df: pd.DataFrame) -> None:
        """
        deletes old values from the passed `df` week and inserts the new
        values passed in `df`

        Args:
            table (str): table to update in
            df (pd.DataFrame): data to use for updating
        Raises:
            Error: any issues with the DB connection
        """
        try:
            weeks = [int(w) for w in df["week"].unique()]
            for week in weeks:
                self._delete_week(week=week, table=table, commit=False)
                self.append_values(
                    table=table, df=df[df["week"] == week], commit=False
                )
            self.conn.commit()
        except Error as e:
            self.conn.rollback()
            self.close_connection()
            raise e

    def get_latest_week(self) -> int:
        """
        Gets latest week available in RDS.

        Returns:
            int: latest week loaded into RDS
        """
        logger.info("Fetching latest week from RDS")
        c: MySQLCursor = self.conn.cursor()
        c.execute("SELECT MAX(week) FROM pulse;")
        result = int(c.fetchone()[0])
        c.close()

        return result

    def get_available_weeks(self) -> tuple[int, ...]:
        """
        Gets all the weeks from RDS

        Returns:
            tuple[int, ...]: All the weeks that are available on the RDS DB.
        """
        logger.info("Fetching set of available weeks in RDS")
        c: MySQLCursor = self.conn.cursor()
        c.execute("SELECT DISTINCT(week) FROM pulse ORDER BY week;")
        result = tuple(int(x[0]) for x in c.fetchall())
        c.close()

        return result

    def get_collection_weeks(self) -> set[int]:
        """
        Gets all the collection weeks from RDS.

        Returns:
            set[int]: All available collection weeks from RDS.
        """
        logger.info("Fetching collection dates from RDS")
        c: MySQLCursor = self.conn.cursor()
        c.execute("SELECT DISTINCT week FROM collection_dates;")
        result = set(int(x[0]) for x in c.fetchall())
        c.close()
        return result

    def get_pulse_dates(self, week: int) -> dict[str, datetime]:
        """
        fetches the collection dates associated with a pulse week

        Args:
            week (int): week to search for

        Returns:
            dict[str, datetime]: a dictionary with the publication, start and
                end dates for a specific survey wave
        """
        logger.info("Fetching collection dates for week %s", week)
        c: MySQLCursor = self.conn.cursor()
        c.execute(
            """
            SELECT *
            FROM collection_dates
            WHERE week = %s
            """,
            (week,),
        )
        colnames = [desc[0] for desc in c.description]
        results = c.fetchall()
        c.close()
        if len(results) == 0:
            error = KeyError(
                f"week {week} not found in collection_dates table"
            )
            raise error
        return dict(zip(colnames, *results))

    def close_connection(self) -> None:
        """
        Closes the connection to the DB
        """
        logger.info("Closing active RDS connection")
        self.conn.close()

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
        logger.info("Fetching entire pulse table from RDS")
        try:
            if query is None:
                query = """SELECT * FROM pulse.pulse;"""

            with warnings.catch_warnings():
                # we ignore the warning that pandas gives us for not using
                # sql alchemy
                warnings.simplefilter("ignore")
                df = pd.read_sql(sql=query, con=self.conn)

                if len(df) == 0:  # pragma: no cover
                    logger.warning("No records found for query %s", query)
        except Error as e:
            self.conn.rollback()
            self.close_connection()
            raise Error(e) from e

        return df

    def get_pulse_with_smoothed(self) -> pd.DataFrame:
        """
        Gets all the records from the pulse table but only those columns
        needed to pre-populate the front end's cache

        Returns:
            pd.DataFrame: pulse columns with the added smoothed pweight share
        """
        logger.info("Fetching pulse table from RDS with smoothed values")
        query = """
            SELECT week,
                xtab_var,
                xtab_val,
                q_var,
                q_val,
                pweight_share,
                pweight_share_smoothed
            FROM pulse
            INNER JOIN smoothed
            USING (week, xtab_var, xtab_val, q_var, q_val);
            """
        with warnings.catch_warnings():
            # we ignore the warning that pandas gives us for not using
            # sql alchemy
            warnings.simplefilter("ignore")
            df = pd.read_sql(sql=query, con=self.conn)
        return df

    def update_collection_dates(self) -> None:
        """
        truncates the current `collection_dates` table and uploads the latest
        collection dates into the RDS database.
        """
        logger.info("Updating collection dates in RDS")
        dl = DataLoader()
        collection_dates = dl.load_collection_dates()
        c: MySQLCursor = self.conn.cursor()
        logger.info("Truncating collection_dates table")
        c.execute("TRUNCATE pulse.collection_dates")
        c.close()

        datdf = pd.DataFrame.from_dict(collection_dates, orient="index")
        datdf.reset_index(inplace=True)
        datdf.rename(columns={"index": "week"}, inplace=True)
        self.append_values(table="collection_dates", df=datdf)

    def _establish_connection(self) -> None:
        """
        starts connection to the DB. gets called automatically when the class
        is instantiated, ability to re run it to reconnect if needed.
        """
        logger.info("Establishing connection to the RDS database")
        self.conn: MySQLConnection = mysql.connector.connect(
            **self._load_rds_creds()
        )

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

    def _delete_week(self, week: int, table: str, commit: bool = True):
        """
        deletes all records that match the passed week value

        Args:
            week (int): the week to remove from the pulse table
            commit (bool): whether to commit the deletion or delay it
        """
        logger.info(
            "Deleting records from RDS table %s for week %s", table, week
        )
        self._refresh_connection()
        query = f"""
            DELETE FROM {table}
            WHERE week = %s
        """
        try:
            c: MySQLCursor = self.conn.cursor()
            c.execute(query, (week,))
            if commit:
                self.conn.commit()
            c.close()
        except Error as error:
            self.conn.rollback()
            c.close()
            self.close_connection()
            raise error

    def _refresh_connection(self) -> None:
        """
        if dead, it tries to reset the connection.
        """
        if not self.conn.is_connected():
            self._establish_connection()

    @staticmethod
    def _load_rds_creds() -> dict[str, str]:
        """
        Loads credentials for RDS MySQL DB from local secrets file

        Returns:
            dict[str, str]: connection config dict
        """
        secret_name = "prod/pulse/rds"
        logger.info("Fetching secret %s from AWS SecretsManager", secret_name)

        session = boto3.Session()
        client = session.client(
            service_name="secretsmanager", region_name="us-east-2"
        )

        try:
            response = client.get_secret_value(SecretId=secret_name)
            creds: dict = json.loads(response["SecretString"])
            return creds
        except ClientError as e:
            raise e from e
