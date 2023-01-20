# -*- coding: utf-8 -*-
"""
Created on Saturday, 8th October 2022 11:45:46 am
===============================================================================
@filename:  test_mysql_wrapper.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   Unit tests for the mysql_wrapper.py module.
===============================================================================
"""
# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=protected-access,no-member

import json
from datetime import date
from typing import Generator, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from mysql.connector import CMySQLConnection

from household_pulse.mysql_wrapper import Error, PulseSQL


@pytest.fixture
def pulsesql() -> Generator[PulseSQL, None, None]:
    with patch(
        "household_pulse.mysql_wrapper.mysql.connector"
    ) as mock_connector:
        with patch.object(
            PulseSQL, "_load_rds_creds", MagicMock(return_value={})
        ):
            mock_connector.connect.return_value = MagicMock(
                name="conn", spec=CMySQLConnection
            )
            pulsesql = PulseSQL()
            yield pulsesql


@pytest.fixture(scope="session")
def mockdf() -> Generator[pd.DataFrame, None, None]:
    df = pd.DataFrame(
        data={"week": [1, 2, 3], "one": [1, 2, 3], "two": [3, 2, 1]}
    )
    yield df


class TestInstantiation:
    """
    Tests the functionality called within the classes __init__ method.
    """

    @staticmethod
    def test_instantiation(pulsesql: PulseSQL) -> None:
        assert pulsesql


class TestClassMethods:
    """
    Unit tests for the class methods
    """

    @staticmethod
    @pytest.mark.parametrize("commit", (False, True))
    def test_append_values(
        commit: bool, pulsesql: PulseSQL, mockdf: pd.DataFrame
    ) -> None:
        with patch.object(PulseSQL, "_refresh_connection", MagicMock()):
            pulsesql.append_values(table="test", df=mockdf, commit=commit)
            pulsesql.conn.cursor.assert_called_once()
            pulsesql._refresh_connection.assert_called_once()
            if commit:
                pulsesql.conn.commit.assert_called_once()

    @staticmethod
    def test_append_values_error(
        pulsesql: PulseSQL, mockdf: pd.DataFrame
    ) -> None:
        pulsesql.conn.cursor.return_value = MagicMock(name="cursor")
        pulsesql.conn.cursor.return_value.executemany = MagicMock(
            name="execute", side_effect=Error
        )
        with pytest.raises(Error):
            pulsesql.append_values(table="test", df=mockdf)

    @staticmethod
    @patch.object(PulseSQL, "_delete_week", MagicMock())
    @patch.object(PulseSQL, "append_values", MagicMock())
    def test_update_values(mockdf: pd.DataFrame, pulsesql: PulseSQL) -> None:
        pulsesql.update_values(table="test", df=mockdf)
        assert pulsesql._delete_week.call_count == 3
        assert pulsesql.append_values.call_count == 3

    @staticmethod
    @patch.object(PulseSQL, "_delete_week", MagicMock(side_effect=Error))
    def test_update_values_error(
        mockdf: pd.DataFrame, pulsesql: PulseSQL
    ) -> None:
        with pytest.raises(Error):
            pulsesql.update_values(table="test", df=mockdf)

    @staticmethod
    def test_get_latest_week(pulsesql: PulseSQL) -> None:
        mock_cursor = MagicMock(name="cursor")
        pulsesql.conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (10, None)
        result = pulsesql.get_latest_week()
        mock_cursor.execute.assert_called_once_with(
            "SELECT MAX(week) FROM pulse;"
        )
        assert result == 10

    @staticmethod
    def test_get_available_weeks(pulsesql: PulseSQL) -> None:
        mock_cursor = MagicMock(name="cursor")
        pulsesql.conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(10, None), (11, None)]
        result = pulsesql.get_available_weeks()
        mock_cursor.execute.assert_called_once_with(
            "SELECT DISTINCT(week) FROM pulse ORDER BY week;"
        )
        assert result == (10, 11)

    @staticmethod
    def test_get_collection_weeks(pulsesql: PulseSQL) -> None:
        mock_cursor = MagicMock(name="cursor")
        pulsesql.conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(10, None), (11, None)]
        result = pulsesql.get_collection_weeks()
        mock_cursor.execute.assert_called_once_with(
            "SELECT DISTINCT week FROM collection_dates;"
        )
        assert result == {10, 11}

    @staticmethod
    def test_get_pulse_dates(pulsesql: PulseSQL) -> None:
        mock_cursor = MagicMock(name="cursor")
        pulsesql.conn.cursor.return_value = mock_cursor
        expected = {
            "week": 10,
            "pub_date": date(2020, 7, 15),
            "start_date": date(2020, 7, 2),
            "end_date": date(2020, 7, 7),
        }
        mock_cursor.description = [
            ("week",),
            ("pub_date",),
            ("start_date",),
            ("end_date",),
        ]
        mock_cursor.fetchall.return_value = [
            (
                10,
                date(2020, 7, 15),
                date(2020, 7, 2),
                date(2020, 7, 7),
            )
        ]
        results = pulsesql.get_pulse_dates(week=10)

        assert results == expected

    @staticmethod
    def test_get_pulse_dates_error(pulsesql: PulseSQL) -> None:
        mock_cursor = MagicMock(name="cursor")
        pulsesql.conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        with pytest.raises(KeyError):
            pulsesql.get_pulse_dates(week=10)

    @staticmethod
    def test_close_connection(pulsesql: PulseSQL) -> None:
        pulsesql.close_connection()
        pulsesql.conn.close.assert_called_once()

    @staticmethod
    @patch.object(pd, "read_sql", MagicMock())
    @pytest.mark.parametrize("query", (None, "SELECT * FROM pulse.pulse;"))
    def test_get_pulse_table(
        query: Optional[str], mockdf: pd.DataFrame, pulsesql: PulseSQL
    ) -> None:
        pd.read_sql.return_value = mockdf
        df = pulsesql.get_pulse_table(query=query)
        assert df.equals(mockdf)
        pd.read_sql.assert_called_with(
            sql="SELECT * FROM pulse.pulse;", con=pulsesql.conn
        )

    @staticmethod
    @patch.object(pd, "read_sql", MagicMock(side_effect=Error))
    def test_get_pulse_table_error(pulsesql: PulseSQL) -> None:
        with pytest.raises(Error):
            pulsesql.get_pulse_table()

    @staticmethod
    @patch.object(pd, "read_sql", MagicMock())
    @pytest.mark.parametrize("query", (None, "SELECT * FROM pulse.pulse;"))
    def test_get_pulse_with_smoothed(
        query: Optional[str], mockdf: pd.DataFrame, pulsesql: PulseSQL
    ) -> None:
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

        pd.read_sql.return_value = mockdf
        df = pulsesql.get_pulse_with_smoothed()
        assert df.equals(mockdf)
        pd.read_sql.assert_called_with(sql=query, con=pulsesql.conn)

    @staticmethod
    @patch.object(PulseSQL, "append_values", MagicMock())
    @patch("household_pulse.mysql_wrapper.DataLoader")
    def test_update_collection_dates(
        mockdl: MagicMock, pulsesql: PulseSQL
    ) -> None:
        pulsesql.update_collection_dates()
        mockdl.load_collection_dates.assert_called_once()
        pulsesql.append_values.assert_called_once()

    @staticmethod
    def test_convert_nans(mockdf: pd.DataFrame, pulsesql: PulseSQL) -> None:
        result = pulsesql._convert_nans(mockdf)
        assert result.isnull().sum().sum() == 0

    @staticmethod
    @pytest.mark.parametrize("commit", (False, True))
    def test_delete_week(commit: bool, pulsesql: PulseSQL) -> None:
        query = """
            DELETE FROM test
            WHERE week = %s
        """
        mock_cursor = MagicMock(name="cursor")
        pulsesql.conn.cursor.return_value = mock_cursor
        pulsesql._delete_week(week=10, table="test", commit=commit)
        mock_cursor.execute.assert_called_once_with(query, (10,))

    @staticmethod
    def test_delete_week_error(pulsesql: PulseSQL) -> None:
        mock_cursor = MagicMock(name="cursor")
        pulsesql.conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Error
        with pytest.raises(Error):
            pulsesql._delete_week(week=10, table="test")

    @staticmethod
    @patch.object(PulseSQL, "_establish_connection", MagicMock())
    def test_refresh_connection(pulsesql: PulseSQL) -> None:
        pulsesql.conn.is_connected.return_value = False
        pulsesql._refresh_connection()
        pulsesql._establish_connection.assert_called_once()

    @staticmethod
    @patch("household_pulse.mysql_wrapper.boto3")
    def test_load_rds_creds(mockboto):
        client = MagicMock(name="client")
        mockboto.Session.return_value.client.return_value = client
        test_creds = {"test": "123", "user": "fake", "password": "12345"}
        test_response = {"SecretString": json.dumps(test_creds)}
        client.get_secret_value.return_value = test_response
        assert test_creds == PulseSQL._load_rds_creds()

    @staticmethod
    @patch("household_pulse.mysql_wrapper.boto3")
    def test_load_rds_creds_error(mockboto):
        client = MagicMock(name="client")
        mockboto.Session.return_value.client.return_value = client
        mockerror = ClientError(
            error_response={"Error": {"Test": "123"}}, operation_name="test"
        )
        test_response = MagicMock(side_effect=mockerror)
        client.get_secret_value = test_response
        with pytest.raises(ClientError):
            PulseSQL._load_rds_creds()
