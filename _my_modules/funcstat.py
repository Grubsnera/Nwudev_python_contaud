"""
Statistical functions
Created on: 29 Aug 2019
Author: Albert J van Rensburg (NWU:21162395)
"""

# IMPORT SYSTEM OBJECTS
import statistics


def stat_mode(o_cursor, s_table, s_column, s_where=""):
    """
    Function to return the statistic mode for a column
    :param o_cursor: Database cursor
    :param s_table: Table in database
    :param s_column: Column to calculate
    :param s_where: Where clause
    :return: Mode statistic
    """

    # Convert the table column into a list and the calculate the mode
    s_sql = "SELECT " + s_column
    s_sql += " FROM " + s_table
    s_sql += " WHERE " + s_column + " Is Not Null"
    if s_where != "":
        s_sql += " And " + s_where
    s_sql += " ;"
    # print(s_sql)
    a_list = []
    for row in o_cursor.execute(s_sql).fetchall():
        a_list.append(row[0])
    return statistics.mode(a_list)


def stat_pstdev(o_cursor, s_table, s_column, s_where=""):
    """
    Function to return the statistic mode for a column
    :param o_cursor: Database cursor
    :param s_table: Table in database
    :param s_column: Column to calculate
    :param s_where: Where clause
    :return: Population standard deviation statistic
    """

    # Convert the table column into a list and the calculate the mode
    s_sql = "SELECT " + s_column
    s_sql += " FROM " + s_table
    s_sql += " WHERE " + s_column + " Is Not Null"
    if s_where != "":
        s_sql += " And " + s_where
    s_sql += " ;"
    # print(s_sql)
    a_list = []
    for row in o_cursor.execute(s_sql).fetchall():
        a_list.append(row[0])
    return statistics.pstdev(a_list)


def stat_highest_value(o_cursor, s_table, s_column, s_where=""):
    """
    Function to return the statistic highest value for a column
    :param o_cursor: Database cursor
    :param s_table: Table in database
    :param s_column: Column to calculate
    :param s_where: Where clause
    :return: Mode statistic
    """

    # Convert the table column into a list and the calculate the mode
    s_sql = "SELECT " + s_column
    s_sql += " FROM " + s_table
    s_sql += " WHERE " + s_column + " Is Not Null"
    if s_where != "":
        s_sql += " And " + s_where
    s_sql += " ORDER BY " + s_column + " DESC "
    s_sql += " ;"
    # print(s_sql)
    row = o_cursor.execute(s_sql).fetchone()
    return row[0]


def stat_list(o_cursor, s_table, s_column, s_where=""):
    """
    Function to return the statistic mode for a column
    :param o_cursor: Database cursor
    :param s_table: Table in database
    :param s_column: Column to return
    :param s_where: Where clause
    :return: List
    """

    # Convert the table column into a list and the calculate the mode
    s_sql = "SELECT " + s_column
    s_sql += " FROM " + s_table
    s_sql += " WHERE " + s_column + " Is Not Null"
    if s_where != "":
        s_sql += " And " + s_where
    s_sql += " ;"
    # print(s_sql)
    a_list = []
    for row in o_cursor.execute(s_sql).fetchall():
        a_list.append(row[0])
    return a_list


def stat_tuple(o_cursor, s_table, s_column, s_where=""):
    """
    Function to return the statistic mode for a column
    :param o_cursor: Database cursor
    :param s_table: Table in database
    :param s_column: Column to return
    :param s_where: Where clause
    :return: List
    """

    # Convert the table column into a list and the calculate the mode
    s_sql = "SELECT " + s_column
    s_sql += " FROM " + s_table
    s_sql += " WHERE " + s_column + " Is Not Null"
    if s_where != "":
        s_sql += " And " + s_where
    s_sql += " ;"
    # print(s_sql)
    a_list = []
    for row in o_cursor.execute(s_sql).fetchall():
        a_list.append(row[0])
    return tuple(a_list)
