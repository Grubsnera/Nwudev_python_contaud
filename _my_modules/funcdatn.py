"""
This module does various date calculations and return them.

It is part of the NWUIACA project, and is used throughout to obtain dates in various formats.

Author: Albert Janse van Rensburg (NWU:21162395)
Date: 2023-12-03
"""

# Import standard libraries
import calendar
from datetime import date, datetime, timedelta

# Import related third party libraries

# Import local application modules

# Metadata
__author__ = 'Albert Janse van Rensburg'
__email__ = '21162395@nwu.ac.za'
__version__ = '1.0.0'
__status__ = 'Development'  # Production

"""
INDEX

get_today_date(): Return today's date in YYYY-MM-DD format.
get_today_date_file(): Return today's date in YYYYMMDD format.
get_today_name(): Return today's weekday name.
get_today_day(): Return today's day as a string in DD format.
get_today_day_strip(): Return today's day as a string in 'D' format.
get_current_month(): Return today's month as a string in DD format.
get_current_month_begin(): Return the first day of the current month in YYYY-MM-DD format
get_current_month_end(): Return the last day of the current month in YYYY-MM-DD format.
get_current_month_end_next(): Return the end of this or the next month depending on the cutoff_days
get_current_month_file(): Get the current month in YYYYMM format.
get_current_year(): Return today's year as a string in YYYY format.
get_current_year_begin(): Return the date for the first day of this year.
get_current_year_end(): Return the date for the last day of this year.
get_yesterday_date(): Return yesterday's date in YYYY-MM-DD format.
get_yesterday_date_file(): Return yesterday's date in YYYYMMDD format.
get_previous_month(): Return the previous month as a string in MM format.
get_previous_month_begin(): Return the first day of the previous month in YYYY-MM-DD format.
get_previous_month_end(): Return the last day of the previous month as a string in YYYY-MM-DD format.
get_previous_month_end_file(): Return the last day of the previous month as a string in YYYYMMDD format.
get_previous_month_file(): Return the last day of the previous month as a string in YYYYMM format.
get_previous_year(): Return previous year as a string in YYYY format.
get_previous_year_begin(): Return the first day of the previous year as a string in YYYY-MM-DD format.
get_previous_year_end(): Return the last day of the previous year as a string in YYYY-MM-DD format.
"""


def get_today_date():
    # Get today's date in YYYY-MM-DD format
    return date.today().strftime("%Y-%m-%d")


def get_today_date_file():
    # Get today's date in YYYY-MM-DD format
    return date.today().strftime("%Y%m%d")


def get_today_name():
    # Get the current local datetime
    today = datetime.now()
    return today.strftime('%a')


def get_today_day():
    """Return today's day as a string in DD format, e.g., '01' for the first day of the month."""
    return date.today().strftime("%d")


def get_today_day_strip():
    """Return today's day as a string in 'D' format, e.g., '1' for the first day of the month."""
    return str(date.today().day)


def get_current_month():
    """Return today's month as a string in MM format, e.g., '01' for the January."""
    return date.today().strftime("%m")


def get_current_month_begin():
    """Return the first day of the current month in YYYY-MM-DD format. Example 2023-04-01"""
    today = datetime.today()
    first_day_of_month = datetime(today.year, today.month, 1)
    return first_day_of_month.strftime("%Y-%m-%d")


def get_current_month_end():
    """Return the last day of the current month in YYYY-MM-DD format. Example 2023-04-30"""
    # Get the current date
    today = datetime.now()
    year = today.year
    month = today.month
    # Find the last day of the month
    last_day = calendar.monthrange(year, month)[1]
    # Return the YYYY-MM-DD formatted date string
    return f"{year}-{month:02d}-{last_day}"


def get_current_month_end_next(cutoff_days: int = 7):
    """
    Function to return the end of this month or the next month.
    :param cutoff_days: Cutoff days
    :return: YYYY-MM-DD
    """
    """
    In essence, this function allows you to determine when the cutoff is for the end of the month, taking into
    account a buffer of a certain number of days specified by cutoff_days. If today’s date is within cutoff_days
    of the end of the current month, it provides the last day of the next month; otherwise, it gives you the last
    day of the current month.
    """

    # Get today's date
    today = date.today()
    year = today.year
    month = today.month
    day = today.day

    # Calculate the last day of the current month
    last_day_of_month = calendar.monthrange(year, month)[1]
    # Create a date for the last day of the current month
    end_of_month = date(year, month, last_day_of_month)

    if (end_of_month - today).days <= cutoff_days:
        # If we're within days of the end of this month, find the end of the next month
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        last_day_of_next_month = calendar.monthrange(year, month)[1]
        end_of_next_month = date(year, month, last_day_of_next_month)
        return end_of_next_month.strftime("%Y-%m-%d")
    else:
        return end_of_month.strftime("%Y-%m-%d")


def get_current_month_file():
    # Get the current month in YYYYMM format
    return date.today().strftime("%Y%m")


def get_current_year():
    """Return today's year as a string in YYYY format, e.g., '2023' for 2023."""
    return date.today().strftime("%Y")


def get_current_year_begin():
    today = date.today()
    first_day_of_year = date(today.year, 1, 1)
    return first_day_of_year


def get_current_year_end():
    today = date.today()
    first_day_of_year = date(today.year, 12, 31)
    return first_day_of_year


def get_yesterday_date():
    """
    YESTERDAY date ex: 2019-09-06
    :return: str: YYYY-MM-DD
    """
    # Compute yesterday's date only once
    yesterday_date = date.today() - timedelta(days=1)
    # Return the date as a string in the format YYYY-MM-DD
    return yesterday_date.strftime("%Y-%m-%d")


def get_yesterday_date_file():
    """
    YESTERDAY date ex: 20190906
    :return: str: YYYYMMDD
    """
    # Compute yesterday's date only once
    yesterday_date = date.today() - timedelta(days=1)
    # Return the date as a string in the format YYYY-MM-DD
    return yesterday_date.strftime("%Y%m%d")


def get_previous_month():
    """Return the previous month as a string in MM format, e.g., '01' for January."""
    today = date.today()
    first_of_this_month = date(today.year, today.month, 1)
    last_day_of_previous_month = first_of_this_month - timedelta(days=1)
    return last_day_of_previous_month.strftime("%m")


def get_previous_month_begin():
    """Return the first day of the previous month in YYYY-MM-DD format."""
    today = datetime.today()
    # Calculate the first day of the current month
    first_day_of_current_month = datetime(today.year, today.month, 1)
    # Subtract one day to get to the last day of the previous month
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    # Now that we have the last day of the previous month, we can extract the year and month
    # and create a new datetime object for the first day of that month
    first_day_of_previous_month = datetime(last_day_of_previous_month.year, last_day_of_previous_month.month, 1)
    return first_day_of_previous_month.strftime("%Y-%m-%d")


def get_previous_month_end():
    """Return the last day of the previous month as a string in YYYY-MM-DD format."""
    today = date.today()
    first_of_this_month = date(today.year, today.month, 1)
    last_day_of_previous_month = first_of_this_month - timedelta(days=1)
    return last_day_of_previous_month.strftime("%Y-%m-%d")


def get_previous_month_end_file():
    """Return the last day of the previous month as a string in YYYYMMDD format."""
    today = date.today()
    first_of_this_month = date(today.year, today.month, 1)
    last_day_of_previous_month = first_of_this_month - timedelta(days=1)
    return last_day_of_previous_month.strftime("%Y%m%d")


def get_previous_month_file():
    """Return the last day of the previous month as a string in YYYYMM format."""
    today = date.today()
    first_of_this_month = date(today.year, today.month, 1)
    last_day_of_previous_month = first_of_this_month - timedelta(days=1)
    return last_day_of_previous_month.strftime("%Y%m")


def get_previous_year():
    """Return previous year as a string in YYYY format, e.g., '2022' for 2023."""
    last_year = date.today().year - 1
    return str(last_year)


def get_previous_year_begin():
    """Return the first day of the previous year as a string in YYYY-MM-DD format."""
    last_year = date.today().year - 1
    first_day_of_year = date(last_year, 1, 1)
    return first_day_of_year


def get_previous_year_end():
    """Return the last day of the previous year as a string in YYYY-MM-DD format."""
    last_year = date.today().year - 1
    first_day_of_year = date(last_year, 12, 31)
    return first_day_of_year


if __name__ == '__main__':
    print(get_today_date())
    print(get_today_date_file())
    print(get_today_name())
    print(get_today_day())
    print(get_today_day_strip())
    print(get_current_month())
    print(get_current_month_begin())
    print(get_current_month_end())
    print(get_current_month_end_next())
    print(get_current_month_end_next(30))
    print(get_current_month_file())
    print(get_current_year())
    print(get_current_year_begin())
    print(get_current_year_end())
    print(get_yesterday_date())
    print(get_yesterday_date_file())
    print(get_previous_month())
    print(get_previous_month_begin())
    print(get_previous_month_end())
    print(get_previous_month_end_file())
    print(get_previous_month_file())
    print(get_previous_year())
    print(get_previous_year_begin())
    print(get_previous_year_end())
