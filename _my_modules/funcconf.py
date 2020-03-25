"""
Script to define global variables
"""

# IMPORT PYTHON PACKAGES
import datetime

# IMPORT OWN MODULES
from _my_modules import funcdate

# DECLARE GLOBAL VARIABLES
l_run_project: bool = True  # Flag to indicate project running or stopping.
d_run_large: datetime = datetime.datetime(int(funcdate.cur_year()),
                                          int(funcdate.cur_month()),
                                          int(funcdate.cur_day()), 18, 0, 0)
d_run_test: datetime = datetime.datetime(int(funcdate.cur_year()),
                                         int(funcdate.cur_month()),
                                         int(funcdate.cur_day()) + 1, 2, 0, 0)
