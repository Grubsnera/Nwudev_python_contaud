"""
Statistical functions
Created on: 29 Aug 2019
Author: Albert J van Rensburg (NWU:21162395)
"""

# IMPORT SYSTEM OBJECTS
import statistics


def stat_mode(o_cursor, s_table, s_column):
    """
    Function to calculate the MODE of any table column
    :return: The table column mode
    """

    # Convert the table column into a list and the calculate the mode
    a_list = []
    for row in o_cursor.execute("SELECT " + s_column + " FROM " + s_table + " WHERE " + s_column + " != 0 ;").fetchall():
        a_list.append(row[0])
    return statistics.mode(a_list)
