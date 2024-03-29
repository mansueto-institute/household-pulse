# -*- coding: utf-8 -*-
"""
Created on Saturday, 22nd October 2022 5:33:55 pm
===============================================================================
@filename:  test_cli.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   Unit tests for the __main__.py module (the CLI).
===============================================================================
"""
# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=protected-access,no-member

from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError

from household_pulse.__main__ import PulseCLI
from household_pulse.io import Census, S3Storage


class TestInstantiation:
    """
    Tests the creation of the CLI
    """

    @staticmethod
    def test_instantiation_bad_args():
        with pytest.raises(SystemExit):
            PulseCLI(args=["bad"])

    @staticmethod
    @pytest.mark.parametrize("subcommand", ("etl", "fetch"))
    def test_subcommands(subcommand: str):
        cli = PulseCLI(args=[subcommand])
        assert cli.args.subcommand == subcommand

    @staticmethod
    @patch.object(PulseCLI, "_etlparser")
    def test_etl_subparser(_etlparser: MagicMock):
        PulseCLI(args=["etl"])
        _etlparser.assert_called_once()

    @staticmethod
    @patch.object(PulseCLI, "_dataparser")
    def test_fetch_subparser(_dataparser: MagicMock):
        PulseCLI(args=["fetch"])
        _dataparser.assert_called_once()


class TestMethods:
    """
    Tests the methods of the PulseCLI class.
    """

    @staticmethod
    @patch.object(PulseCLI, "etl_subcommand", MagicMock())
    def test_main_etl():
        cli = PulseCLI(args=["etl"])
        cli.main()
        cli.etl_subcommand.assert_called_once()

    @staticmethod
    @patch.object(PulseCLI, "fetch_subcommand", MagicMock())
    def test_main_fetch():
        cli = PulseCLI(args=["fetch"])
        cli.main()
        cli.fetch_subcommand.assert_called_once()

    @staticmethod
    @patch.object(PulseCLI, "_resolve_outpath", MagicMock())
    @patch("household_pulse.__main__.S3Storage")
    def test_download_pulse(mocks3: MagicMock):
        cli = PulseCLI(args=["fetch", "download-pulse", "test"])
        s3 = mocks3.return_value
        cli.download_pulse()
        cli._resolve_outpath.assert_called_once()  # type: ignore
        s3.download_all.assert_called_once()

    @staticmethod
    @patch.object(PulseCLI, "_resolve_outpath", MagicMock())
    @patch("household_pulse.__main__.S3Storage")
    def test_download_pulse_with_week(mocks3: MagicMock):
        cli = PulseCLI(
            args=["fetch", "download-pulse", "test", "--week", "10"]
        )
        s3 = mocks3.return_value
        cli.download_pulse()
        cli._resolve_outpath.assert_called_once()  # type: ignore
        s3.download_parquet.assert_called_once()

    @staticmethod
    @patch.object(PulseCLI, "_resolve_outpath", MagicMock())
    @patch("household_pulse.__main__.Pulse")
    def test_download_raw(mock_pulse: MagicMock):
        cli = PulseCLI(args=["fetch", "download-raw", "test", "--week", "10"])
        pulse = mock_pulse.return_value
        cli.download_raw()
        cli._resolve_outpath.assert_called_once()  # type: ignore
        pulse.download_data.assert_called_once()

    @staticmethod
    @pytest.mark.parametrize("target", ("s3", "census", "bad"))
    @patch("household_pulse.__main__.S3Storage")
    @patch("household_pulse.__main__.Census")
    def test_get_latest_week(
        mock_census: MagicMock, mock_s3: MagicMock, target: str
    ):
        cli = PulseCLI(args=["etl", "--get-latest-week", target])
        if target == "bad":
            with pytest.raises(SystemExit):
                cli.get_latest_week(target=target)
        elif target == "s3":
            mock_s3.return_value.get_available_weeks.return_value = {39, 40}
            assert cli.get_latest_week(target=target) == 40
        else:
            mock_census.get_week_year_map.return_value = {
                39: 2022,
                40: 2022,
            }
            assert cli.get_latest_week(target=target) == 40

    @staticmethod
    @pytest.mark.parametrize("target", ("s3", "census", "bad"))
    @patch("household_pulse.__main__.S3Storage")
    @patch("household_pulse.__main__.Census")
    def test_get_all_weeks(
        mock_census: MagicMock, mock_s3: MagicMock, target: str
    ):
        cli = PulseCLI(args=["etl", "--get-all-weeks", target])
        if target == "bad":
            with pytest.raises(SystemExit):
                cli.get_all_weeks(target=target)
        elif target == "s3":
            mock_s3.return_value.get_available_weeks.return_value = (40, 39)
            assert cli.get_all_weeks(target=target) == (39, 40)
        else:
            mock_census.get_week_year_map.return_value = {40: 2022, 39: 2022}
            assert cli.get_all_weeks(target=target) == (39, 40)

    @staticmethod
    @pytest.mark.parametrize(
        "filepath, file_prefix, week",
        (("bad", "test", None), (".", "test", None), (".", "test", 10)),
    )
    def test_resolve_outpath(
        filepath: str, file_prefix: str, week: Optional[int]
    ):
        if filepath == "bad":
            with pytest.raises(FileNotFoundError):
                PulseCLI._resolve_outpath(
                    filepath=filepath, file_prefix=file_prefix, week=week
                )
        else:
            path: Path = PulseCLI._resolve_outpath(
                filepath=filepath, file_prefix=file_prefix, week=week
            )
            if week is None:
                assert path.name == "test.csv"
            else:
                assert path.name == "test-10.csv"

    @staticmethod
    @patch("household_pulse.__main__.requests")
    @patch("boto3.session.Session.client")
    def test_build_request(mock_client: MagicMock, mock_requests: MagicMock):
        mock_client.return_value.get_secret_value.return_value = {
            "SecretString": '{"token": "123"}'
        }
        mock_requests.get.return_value.json.return_value = {"123": "123"}
        PulseCLI._build_request()
        mock_client.assert_called_once()
        mock_client.return_value.get_secret_value.assert_called_once_with(
            SecretId="prod/pulse/github"
        )

    @staticmethod
    @patch("boto3.session.Session.client")
    def test_build_request_no_secret(mock_client: MagicMock):
        mock_client.return_value.get_secret_value.side_effect = ClientError(
            error_response={"Error": {"Code": "ResourceNotFoundException"}},
            operation_name="get_secret_value",
        )
        with pytest.raises(ClientError):
            PulseCLI._build_request()
        mock_client.assert_called_once()
        mock_client.return_value.get_secret_value.assert_called_once_with(
            SecretId="prod/pulse/github"
        )


class TestETLSubcommand:
    """
    Tests all the logic for the etl subcommand.
    """

    @staticmethod
    @pytest.mark.parametrize("target", ("s3", "census"))
    @patch.object(PulseCLI, "get_latest_week")
    def test_etl_get_latest_week(mock_method: MagicMock, target: str, capsys):
        mock_method.return_value = 10
        cli = PulseCLI(args=["etl", "--get-latest-week", target])
        cli.etl_subcommand()
        captured = capsys.readouterr()
        assert captured.out == f"Latest week available on {target} is 10\n"

    @staticmethod
    @pytest.mark.parametrize("target", ("s3", "census"))
    @patch.object(PulseCLI, "get_all_weeks")
    def test_etl_get_all_weeks(mock_method: MagicMock, target: str, capsys):
        mock_method.return_value = (10, 12, 13)
        cli = PulseCLI(args=["etl", "--get-all-weeks", target])
        cli.etl_subcommand()
        captured = capsys.readouterr()
        assert (
            captured.out == f"Available weeks on {target} are (10, 12, 13)\n"
        )

    @staticmethod
    @patch("household_pulse.__main__.Pulse")
    def test_etl_run_single_week(mock_pulse: MagicMock) -> None:
        cli = PulseCLI(args=["etl", "--run-single-week", "40"])
        cli.etl_subcommand()
        pulse: MagicMock = mock_pulse.return_value
        mock_pulse.assert_called_once_with(week=40)
        pulse.process_data.assert_called_once()
        pulse.upload_data.assert_called_once()

    @staticmethod
    @patch("household_pulse.__main__.Pulse")
    @patch.object(PulseCLI, "get_latest_week", MagicMock(return_value=40))
    def test_etl_run_latest_week(mock_pulse: MagicMock) -> None:
        cli = PulseCLI(args=["etl", "--run-latest-week"])
        cli.etl_subcommand()
        pulse: MagicMock = mock_pulse.return_value
        mock_pulse.assert_called_once_with(week=40)
        pulse.process_data.assert_called_once()
        pulse.upload_data.assert_called_once()

    @staticmethod
    @patch("household_pulse.__main__.Pulse")
    def test_etl_run_multiple_weeks(mock_pulse: MagicMock) -> None:
        cli = PulseCLI(args=["etl", "--run-multiple-weeks", "40", "41"])
        cli.etl_subcommand()
        calls = [
            call(week=40),
            call().process_data(),
            call().upload_data(),
            call(week=41),
            call().process_data(),
            call().upload_data(),
        ]
        mock_pulse.assert_has_calls(calls)

    @staticmethod
    @patch("household_pulse.__main__.Pulse")
    @patch.object(PulseCLI, "get_all_weeks", MagicMock(return_value=(40, 41)))
    def test_etl_run_all_weeks(mock_pulse: MagicMock) -> None:
        cli = PulseCLI(args=["etl", "--run-all-weeks"])
        cli.etl_subcommand()
        calls = [
            call(week=40),
            call().process_data(),
            call().upload_data(),
            call(week=41),
            call().process_data(),
            call().upload_data(),
        ]
        mock_pulse.assert_has_calls(calls)

    @staticmethod
    @patch("household_pulse.__main__.Pulse")
    @patch("household_pulse.__main__.S3Storage")
    @patch("household_pulse.__main__.Census")
    def test_etl_backfill(
        mock_census: MagicMock, mock_s3: MagicMock, mock_pulse: MagicMock
    ):
        mock_census.get_week_year_map.return_value = {
            40: 2022,
            41: 2022,
        }
        mock_s3.return_value.get_available_weeks.return_value = {40}
        cli = PulseCLI(args=["etl", "--backfill"])
        cli.etl_subcommand()
        calls = [
            call(week=41),
            call().process_data(),
            call().upload_data(),
        ]
        mock_pulse.assert_has_calls(calls)

    @staticmethod
    @patch("household_pulse.__main__.smooth_pulse")
    def test_etl_smooth_pulse(mock_smooth: MagicMock):
        cli = PulseCLI(args=["etl", "--run-smoothing"])
        cli.etl_subcommand()
        mock_smooth.assert_called_once()

    @staticmethod
    @patch("household_pulse.__main__.build_front_cache")
    def test_etl_build_front_cache(mock_build_cache: MagicMock):
        cli = PulseCLI(args=["etl", "--build-front-cache"])
        cli.etl_subcommand()
        mock_build_cache.assert_called_once()

    @staticmethod
    @patch.object(PulseCLI, "_build_request")
    def test_etl_send_build_request(mock_build: MagicMock):
        cli = PulseCLI(args=["etl", "--send-build-request"])
        cli.etl_subcommand()
        mock_build.assert_called_once()


class TestFetchSubcommand:
    """
    Tests all the logic for the fetch subcommand.
    """

    @staticmethod
    @patch.object(PulseCLI, "download_pulse")
    def test_fetch_download_pulse(mock_download: MagicMock):
        cli = PulseCLI(args=["fetch", "download-pulse", "test"])
        cli.fetch_subcommand()
        mock_download.assert_called_once()

    @staticmethod
    @patch.object(PulseCLI, "download_raw")
    def test_fetch_download_raw(mock_download: MagicMock):
        cli = PulseCLI(args=["fetch", "download-raw", "test", "--week", "40"])
        cli.fetch_subcommand()
        mock_download.assert_called_once()
