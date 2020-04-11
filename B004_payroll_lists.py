"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
"""

# Import python modules
import sqlite3

# Import own modules
from _my_modules import funcconf
from _my_modules import funcdate
from _my_modules import funccsv
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys


def payroll_lists():
    """
    Script to build payroll lists
    :return: Nothing
    """

    """ CONTENTS ***************************************************************
    ELEMENTS
    BALANCES
    SECONDARY ASSIGNMENTS
    *************************************************************************"""

    # Declare variables
    so_path: str = "W:/People_payroll/"  # Source database path
    so_file: str = "People_payroll.sqlite"  # Source database
    re_path = "R:/People/" #Results
    ed_path = "S:/_external_data/"
    s_sql = "" #SQL statements

    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B004_PAYROLL_LISTS")
    funcfile.writelog("--------------------------")
    print("------------------")
    print("B004_PAYROLL_LISTS")
    print("------------------")

    # MESSAGE
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b>Payroll lists.</b>")

    # Open the SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # Attach data sources
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """*************************************************************************
    START
    *************************************************************************"""

    """*************************************************************************
    ELEMENTS CURRENT
    *************************************************************************"""
    print("---------- ELEMENTS CURRENT ----------")

    # Build the current element list *******************************************
    print("Build the current element list...")
    sr_file = "X000aa_element_list_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        PEE.ASSIGNMENT_ID,
        PEE.ELEMENT_ENTRY_ID,
        PEE.EFFECTIVE_START_DATE,
        PEE.EFFECTIVE_END_DATE,
        PEE.ELEMENT_LINK_ID,
        PEE.CREATOR_TYPE,
        PEE.ENTRY_TYPE,
        PEE.ELEMENT_TYPE_ID,
        PEV.ELEMENT_ENTRY_VALUE_ID,
        PEV.INPUT_VALUE_ID,
        PEV.SCREEN_ENTRY_VALUE,
        PET.ELEMENT_NAME,
        PET.REPORTING_NAME,
        PET.DESCRIPTION
    FROM
        PAY_ELEMENT_ENTRIES_F_CURR PEE LEFT JOIN
        PAY_ELEMENT_ENTRY_VALUES_F_CURR PEV ON PEV.ELEMENT_ENTRY_ID = PEE.ELEMENT_ENTRY_ID AND
            PEV.EFFECTIVE_START_DATE <= PEE.EFFECTIVE_START_DATE AND
            PEV.EFFECTIVE_END_DATE >= PEE.EFFECTIVE_START_DATE LEFT JOIN
        PAY_ELEMENT_TYPES_F PET ON PET.ELEMENT_TYPE_ID = PEE.ELEMENT_TYPE_ID AND
            PET.EFFECTIVE_START_DATE <= PEE.EFFECTIVE_START_DATE AND
            PET.EFFECTIVE_END_DATE >= PEE.EFFECTIVE_START_DATE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X000aa_element_list")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    #s_sql = s_sql.replace("%PMONTH%",funcdate.prev_month())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Extract the NWU TOTAL PACKAGE element for export *********************
    print("Extract the nwu total package element...")
    sr_file = "X001aa_element_package_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X000aa_element_list_curr.ASSIGNMENT_ID,
      X000aa_element_list_curr.EFFECTIVE_START_DATE,
      X000aa_element_list_curr.INPUT_VALUE_ID,
      X000aa_element_list_curr.SCREEN_ENTRY_VALUE,
      X000aa_element_list_curr.ELEMENT_NAME,
      SUBSTR(PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_NUMBER,1,8) AS EMPL_NUMB
    FROM
      X000aa_element_list_curr
      LEFT JOIN PEOPLE.PER_ALL_ASSIGNMENTS_F ON PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_ID = X000aa_element_list_curr.ASSIGNMENT_ID AND
        PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE <= Date('%TODAY%') AND
        PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE >= Date('%TODAY%')
    WHERE
      X000aa_element_list_curr.INPUT_VALUE_ID = 691 AND
      X000aa_element_list_curr.EFFECTIVE_START_DATE <= Date('%TODAY%') AND
      X000aa_element_list_curr.EFFECTIVE_END_DATE >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Build the NWU TOTAL PACKAGE export file **************************************
    print("Build the nwu total package element export file...")
    sr_file = "X001ax_element_package_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X001aa_element_package_curr.EMPL_NUMB,
      X001aa_element_package_curr.EFFECTIVE_START_DATE AS DATE,
      CAST(X001aa_element_package_curr.SCREEN_ENTRY_VALUE AS REAL) AS PACKAGE
    FROM
      X001aa_element_package_curr
    ORDER BY
      X001aa_element_package_curr.EMPL_NUMB
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export the data
    print("Export packages...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Payroll_001ax_package_"
    sx_filet = sx_file + funcdate.cur_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """*************************************************************************
    ELEMENTS PREVIOUS
    *************************************************************************"""
    print("---------- ELEMENTS PREVIOUS ----------")

    # Build the previous element list *******************************************
    print("Build the previous element list...")
    sr_file = "X000aa_element_list_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      PAY_ELEMENT_ENTRIES_F_PREV.ASSIGNMENT_ID,
      PAY_ELEMENT_ENTRIES_F_PREV.ELEMENT_ENTRY_ID,
      PAY_ELEMENT_ENTRIES_F_PREV.EFFECTIVE_START_DATE,
      PAY_ELEMENT_ENTRIES_F_PREV.EFFECTIVE_END_DATE,
      PAY_ELEMENT_ENTRIES_F_PREV.ELEMENT_LINK_ID,
      PAY_ELEMENT_ENTRIES_F_PREV.CREATOR_TYPE,
      PAY_ELEMENT_ENTRIES_F_PREV.ENTRY_TYPE,
      PAY_ELEMENT_ENTRIES_F_PREV.ELEMENT_TYPE_ID,
      PAY_ELEMENT_ENTRY_VALUES_F_PREV.ELEMENT_ENTRY_VALUE_ID,
      PAY_ELEMENT_ENTRY_VALUES_F_PREV.INPUT_VALUE_ID,
      PAY_ELEMENT_ENTRY_VALUES_F_PREV.SCREEN_ENTRY_VALUE,
      PAY_ELEMENT_TYPES_F.ELEMENT_NAME,
      PAY_ELEMENT_TYPES_F.REPORTING_NAME,
      PAY_ELEMENT_TYPES_F.DESCRIPTION
    FROM
      PAY_ELEMENT_ENTRIES_F_PREV
      LEFT JOIN PAY_ELEMENT_ENTRY_VALUES_F_PREV ON PAY_ELEMENT_ENTRY_VALUES_F_PREV.ELEMENT_ENTRY_ID =
        PAY_ELEMENT_ENTRIES_F_PREV.ELEMENT_ENTRY_ID AND PAY_ELEMENT_ENTRY_VALUES_F_PREV.EFFECTIVE_START_DATE <=
        PAY_ELEMENT_ENTRIES_F_PREV.EFFECTIVE_START_DATE AND PAY_ELEMENT_ENTRY_VALUES_F_PREV.EFFECTIVE_END_DATE >= PAY_ELEMENT_ENTRIES_F_PREV.EFFECTIVE_START_DATE
      LEFT JOIN PAY_ELEMENT_TYPES_F ON PAY_ELEMENT_TYPES_F.ELEMENT_TYPE_ID = PAY_ELEMENT_ENTRIES_F_PREV.ELEMENT_TYPE_ID AND
        PAY_ELEMENT_TYPES_F.EFFECTIVE_START_DATE <= PAY_ELEMENT_ENTRIES_F_PREV.EFFECTIVE_START_DATE AND PAY_ELEMENT_TYPES_F.EFFECTIVE_END_DATE >=
        PAY_ELEMENT_ENTRIES_F_PREV.EFFECTIVE_START_DATE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Extract the previous NWU TOTAL PACKAGE element for export ****************
    print("Extract the previous nwu total package element...")
    sr_file = "X001aa_element_package_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X000aa_element_list_prev.ASSIGNMENT_ID,
      X000aa_element_list_prev.EFFECTIVE_START_DATE,
      X000aa_element_list_prev.INPUT_VALUE_ID,
      X000aa_element_list_prev.SCREEN_ENTRY_VALUE,
      X000aa_element_list_prev.ELEMENT_NAME,
      SUBSTR(PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_NUMBER,1,8) AS EMPL_NUMB
    FROM
      X000aa_element_list_prev
      LEFT JOIN PEOPLE.PER_ALL_ASSIGNMENTS_F ON PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_ID = X000aa_element_list_prev.ASSIGNMENT_ID AND
        PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE <= Date('%PYEARE%') AND
        PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE >= Date('%PYEARE%')
    WHERE
      X000aa_element_list_prev.INPUT_VALUE_ID = 691 AND
      X000aa_element_list_prev.EFFECTIVE_START_DATE <= Date('%PYEARE%') AND
      X000aa_element_list_prev.EFFECTIVE_END_DATE >= Date('%PYEARE%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PYEARE%",funcdate.prev_yearend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Build the previous NWU TOTAL PACKAGE export file *************************
    print("Build the previous nwu total package element export file...")
    sr_file = "X001ax_element_package_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X001aa_element_package_prev.EMPL_NUMB,
      X001aa_element_package_prev.EFFECTIVE_START_DATE AS DATE,
      CAST(X001aa_element_package_prev.SCREEN_ENTRY_VALUE AS REAL) AS PACKAGE
    FROM
      X001aa_element_package_prev
    ORDER BY
      X001aa_element_package_prev.EMPL_NUMB
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export the data
    print("Export previous packages...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.prev_year() + "/"
    sx_file = "Payroll_001ax_package_"
    #sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    #funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """*************************************************************************
    BALANCES CURRENT
    *************************************************************************"""
    print("---------- BALANCES CURRENT ----------")

    # Build the balances list ******************************************************
    print("Build the balances list...")
    sr_file = "X000aa_balance_list_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      PAY_RUN_BALANCES_CURR.ASSIGNMENT_ID,
      PAY_RUN_BALANCES_CURR.EFFECTIVE_DATE,
      PAY_RUN_BALANCES_CURR.BALANCE_VALUE,
      PAY_RUN_BALANCES_CURR.RUN_BALANCE_ID,
      PAY_RUN_BALANCES_CURR.DEFINED_BALANCE_ID,
      PAY_DEFINED_BALANCES.BALANCE_TYPE_ID,
      PAY_BALANCE_TYPES.BALANCE_NAME,
      PAY_BALANCE_TYPES.REPORTING_NAME,
      PAY_BALANCE_TYPES.BALANCE_UOM,
      PAY_BALANCE_TYPES.BALANCE_CATEGORY_ID
    FROM
      PAY_RUN_BALANCES_CURR
      LEFT JOIN PAY_DEFINED_BALANCES ON PAY_DEFINED_BALANCES.DEFINED_BALANCE_ID = PAY_RUN_BALANCES_CURR.DEFINED_BALANCE_ID
      LEFT JOIN PAY_BALANCE_TYPES ON PAY_BALANCE_TYPES.BALANCE_TYPE_ID = PAY_DEFINED_BALANCES.BALANCE_TYPE_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Extract the NWU INCOME PER MONTH balance for export **************************
    print("Extract the nwu total income balance...")
    sr_file = "X002aa_balance_totalincome_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X000aa_balance_list_curr.ASSIGNMENT_ID,
      X000aa_balance_list_curr.EFFECTIVE_DATE,
      X000aa_balance_list_curr.DEFINED_BALANCE_ID,
      X000aa_balance_list_curr.BALANCE_VALUE,
      X000aa_balance_list_curr.BALANCE_NAME,
      X000aa_balance_list_curr.REPORTING_NAME,
      SUBSTR(PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_NUMBER,1,8) AS EMPL_NUMB
    FROM
      X000aa_balance_list_curr
      LEFT JOIN PEOPLE.PER_ALL_ASSIGNMENTS_F ON PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_ID = X000aa_balance_list_curr.ASSIGNMENT_ID AND
        PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE <= Date('%PMONTHEND%') AND
        PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE >= Date('%PMONTHEND%')
    WHERE
      X000aa_balance_list_curr.DEFINED_BALANCE_ID = 16264 AND
      X000aa_balance_list_curr.EFFECTIVE_DATE = Date('%PMONTHEND%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PMONTHEND%",funcdate.prev_monthend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Build the NWU TOTAL INCOME export file ***************************************
    print("Build the nwu total income balance export file...")
    sr_file = "X002ax_balance_totalincome_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X002aa_balance_totalincome_curr.EMPL_NUMB,
      X002aa_balance_totalincome_curr.EFFECTIVE_DATE AS DATE,
      CAST(X002aa_balance_totalincome_curr.BALANCE_VALUE AS REAL) AS INCOME
    FROM
      X002aa_balance_totalincome_curr
    ORDER BY
      X002aa_balance_totalincome_curr.EMPL_NUMB
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export the data
    print("Export incomes...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Payroll_002ax_income_total_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """*************************************************************************
    BALANCES PREVIOUS
    *************************************************************************"""
    print("---------- BALANCES PREVIOUS ----------")

    # Build the previous balances list *********************************************
    print("Build the previous balances list...")
    sr_file = "X000aa_balance_list_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      PAY_RUN_BALANCES_PREV.ASSIGNMENT_ID,
      PAY_RUN_BALANCES_PREV.EFFECTIVE_DATE,
      PAY_RUN_BALANCES_PREV.BALANCE_VALUE,
      PAY_RUN_BALANCES_PREV.RUN_BALANCE_ID,
      PAY_RUN_BALANCES_PREV.DEFINED_BALANCE_ID,
      PAY_DEFINED_BALANCES.BALANCE_TYPE_ID,
      PAY_BALANCE_TYPES.BALANCE_NAME,
      PAY_BALANCE_TYPES.REPORTING_NAME,
      PAY_BALANCE_TYPES.BALANCE_UOM,
      PAY_BALANCE_TYPES.BALANCE_CATEGORY_ID
    FROM
      PAY_RUN_BALANCES_PREV
      LEFT JOIN PAY_DEFINED_BALANCES ON PAY_DEFINED_BALANCES.DEFINED_BALANCE_ID = PAY_RUN_BALANCES_PREV.DEFINED_BALANCE_ID
      LEFT JOIN PAY_BALANCE_TYPES ON PAY_BALANCE_TYPES.BALANCE_TYPE_ID = PAY_DEFINED_BALANCES.BALANCE_TYPE_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Extract the previous NWU INCOME PER MONTH balance for export *****************
    print("Extract the previous nwu total income balance...")
    sr_file = "X002aa_balance_totalincome_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X000aa_balance_list_prev.ASSIGNMENT_ID,
      X000aa_balance_list_prev.EFFECTIVE_DATE,
      X000aa_balance_list_prev.DEFINED_BALANCE_ID,
      X000aa_balance_list_prev.BALANCE_VALUE,
      X000aa_balance_list_prev.BALANCE_NAME,
      X000aa_balance_list_prev.REPORTING_NAME,
      SUBSTR(PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_NUMBER,1,8) AS EMPL_NUMB
    FROM
      X000aa_balance_list_prev
      LEFT JOIN PEOPLE.PER_ALL_ASSIGNMENTS_F ON PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_ID = X000aa_balance_list_prev.ASSIGNMENT_ID AND
        PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE <= Date('%PYEARE%') AND
        PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE >= Date('%PYEARE%')
    WHERE
      X000aa_balance_list_prev.DEFINED_BALANCE_ID = 16264 AND
      X000aa_balance_list_prev.EFFECTIVE_DATE = Date('%PYEARE%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PYEARE%",funcdate.prev_yearend())
    #print(s_sql)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Build the previous NWU TOTAL INCOME export file ******************************
    print("Build the previous nwu total income balance export file...")
    sr_file = "X002ax_balance_totalincome_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X002aa_balance_totalincome_prev.EMPL_NUMB,
      X002aa_balance_totalincome_prev.EFFECTIVE_DATE AS DATE,
      CAST(X002aa_balance_totalincome_prev.BALANCE_VALUE AS REAL) AS INCOME
    FROM
      X002aa_balance_totalincome_prev
    ORDER BY
      X002aa_balance_totalincome_prev.EMPL_NUMB
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export the data
    print("Export previous incomes...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.prev_year() + "/"
    sx_file = "Payroll_002ax_income_total_"
    #sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    #funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """*************************************************************************
    SECONDARY ASSIGNMENTS
    *************************************************************************"""
    print("---------- SECONDARY ASSIGNMENTS ----------")

    # Build previous secondary assignments *************************************
    print("Build previous secondary assignments...")
    sr_file = "X000aa_sec_assignment_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.ASSIGNMENT_EXTRA_INFO_ID,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.ASSIGNMENT_ID,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION1,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION2,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION3,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION4,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION5,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION6,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION7,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION8,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION9,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION10,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION11,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION12 AS DATE_FROM,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION13 AS DATE_TO,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION14,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION15,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION16,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION17,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION18,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION19,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION20,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION21,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION22,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION23,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION24,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION25,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION26,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION27,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION28,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION29,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.AEI_INFORMATION30,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.LAST_UPDATE_DATE,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.LAST_UPDATED_BY,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.LAST_UPDATE_LOGIN,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.CREATED_BY,
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC.CREATION_DATE
    FROM
      PEOPLE.PER_ASSIGNMENT_EXTRA_INFO_SEC
    WHERE
      DATE_FROM <= Date('2018-12-31') AND
      DATE_TO >= Date('2018-12-31')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute("DROP TABLE IF EXISTS X000aa_sec_assignment_prev")
    s_sql = s_sql.replace("%PYEARE%",funcdate.prev_yearend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    End OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # COMMIT DATA
    so_conn.commit()

    # CLOSE THE DATABASE CONNECTION
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("-----------------------------")
    funcfile.writelog("COMPLETED: B004_PAYROLL_LISTS")

    return


if __name__ == '__main__':
    try:
        payroll_lists()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "B004_payroll_lists", "B004_payroll_lists")
