# -*- coding: utf-8 -*-
"""
Created on 2023-01-20 05:28:42-06:00
===============================================================================
@filename:  base.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   base class for census and s3 classes.
===============================================================================
"""
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class IO:
    """
    This class is the abstract base class for all the input/output classes.
    """

    week: int
    _week: int = field(init=False, repr=False)

    @property  # type: ignore
    def week(self) -> int:
        """
        Gets the census week value.

        Returns:
            int: The census week value.
        """
        return self._week

    @week.setter
    def week(self, value: int) -> None:
        if isinstance(value, property):
            raise TypeError(
                "__init__() missing 1 required positional argument: 'week'"
            )
        if not isinstance(value, int):
            raise TypeError(f"week must be an integer, not {type(value)}")
        self._week = value

    @property
    def week_str(self) -> str:
        """
        Returns the week as a string with leading zeros.

        Returns:
            str: The week as a string with leading zeros.
        """
        return f"{str(self.week).zfill(2)}"
