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
    PAYROLL RUN VALUES
    ELEMENTS
    BALANCES
    SECONDARY ASSIGNMENTS
    *************************************************************************"""

    # Declare variables
    so_path: str = "W:/People_payroll/"  # Source database path
    so_file: str = "People_payroll.sqlite"  # Source database
    re_path = "R:/People/"  # Results
    ed_path = "S:/_external_data/"
    s_sql = ""  # SQL statements
    l_export: bool = False

    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B004_PAYROLL_LISTS")
    funcfile.writelog("--------------------------")
    print("------------------")
    print("B004_PAYROLL_LISTS")
    print("------------------")

    # MESSAGE
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b>B004 Payroll lists</b>")

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
    PAYROLL RUN VALUES
    *************************************************************************"""

    # For which year
    s_year: str = 'curr'
    if s_year == 'curr':
        year_start: str = funcdate.cur_yearbegin()
        year_end: str = funcdate.cur_yearend()
        s_table_name: str = 'Payroll history curr'
    elif s_year == 'prev':
        year_start: str = funcdate.prev_yearbegin()
        year_end: str = funcdate.prev_yearend()
        s_table_name: str = 'Payroll history prev'
    else:
        year_start: str = s_year + '-01-01'
        year_end: str = s_year + '-12-31'
        s_table_name: str = 'Payroll history ' + s_year

    # Build the Oracle sql statement
    s_sql: str = """
    Select Distinct
        prr.RUN_RESULT_ID,
        pect.CLASSIFICATION_NAME,
        petf.ELEMENT_NAME,
        petf.REPORTING_NAME,
        ppa.EFFECTIVE_DATE,
        Cast(prrv.RESULT_VALUE As Decimal(13,2)) As RESULT_VALUE,
        paaf.LOCATION_ID,
        paaf.ORGANIZATION_ID,
        paaf.EMPLOYMENT_CATEGORY,
        paaf.POSITION_ID,
        paaf.EMPLOYEE_CATEGORY,
        paa.ASSIGNMENT_ID,
        papf.PERSON_ID,
        papf.EMPLOYEE_NUMBER
        --papf.EMPLOYEE_NUMBER,
        --papf.FULL_NAME    
        --piv.UOM,
        --piv.NAME,
        --petf.ELEMENT_TYPE_ID,
        --petf.EFFECTIVE_START_DATE,
        --petf.EFFECTIVE_END_DATE,
        --petf.DESCRIPTION As ELEMENT_DESCRIPTION,
        --pect.DESCRIPTION As CLASSIFICATION_DESCRIPTION,
        --paa.ASSIGNMENT_ACTION_ID,
        --paa.PAYROLL_ACTION_ID,
        --paa.PAYROLL_ACTION_ID#1,
        --paa.ACTION_STATUS,
        --ppa.PAYROLL_ACTION_ID As PAYROLL_ACTION_PPA,
        --ppa.ACTION_TYPE,
        --ppa.ACTION_SEQUENCE,
        --paaf.ASSIGNMENT_ID As ASSIGNMENT_PAAF,
        --paaf.EFFECTIVE_START_DATE As EFFECTIVE_START_PAAF,
        --paaf.EFFECTIVE_END_DATE As EFFECTIVE_END_PAAF,
        --papf.EFFECTIVE_START_DATE As EFFECTIVE_START_PAPF,
        --papf.EFFECTIVE_END_DATE As EFFECTIVE_END_PAPF,
    From
        HR.PAY_RUN_RESULTS prr,
        HR.PAY_RUN_RESULT_VALUES prrv,
        HR.PAY_INPUT_VALUES_F piv,
        HR.PAY_ELEMENT_TYPES_F petf,
        HR.PAY_ELEMENT_CLASSIFICATIONS_TL pect,
        HR.PAY_ASSIGNMENT_ACTIONS paa,
        APPS.PAY_PAYROLL_ACTIONS ppa,
        APPS.PER_ALL_ASSIGNMENTS_F paaf,
        HR.PER_ALL_PEOPLE_F papf  
    Where
        prr.RUN_RESULT_ID = prrv.RUN_RESULT_ID And
        piv.INPUT_VALUE_ID = prrv.INPUT_VALUE_ID And
        prrv.RESULT_VALUE != '0' And
        piv.UOM = 'M' And
        piv.NAME = 'Pay Value' And
        petf.ELEMENT_TYPE_ID = prr.ELEMENT_TYPE_ID And
        pect.CLASSIFICATION_ID = petf.CLASSIFICATION_ID And
        --pect.CLASSIFICATION_NAME In ('Normal Income', 'Allowances') And
        prr.ASSIGNMENT_ACTION_ID = paa.ASSIGNMENT_ACTION_ID And    
        paa.ASSIGNMENT_ID = paaf.ASSIGNMENT_ID And
        papf.PERSON_ID = paaf.PERSON_ID And
        paa.PAYROLL_ACTION_ID#1 = ppa.ACTION_SEQUENCE - 1 And
        paa.ACTION_STATUS = 'C' And
        ppa.ACTION_TYPE In ('R') And
        ppa.EFFECTIVE_DATE >= To_Date('%BEGIN%', 'YYYY-MM-DD') And
        ppa.EFFECTIVE_DATE <= To_Date('%END%', 'YYYY-MM-DD') And
        ppa.EFFECTIVE_DATE >= Trunc(paaf.EFFECTIVE_START_DATE) And
        ppa.EFFECTIVE_DATE <= Trunc(paaf.EFFECTIVE_END_DATE) And
        Trunc(paaf.EFFECTIVE_END_DATE) >= papf.EFFECTIVE_START_DATE And
        Trunc(paaf.EFFECTIVE_END_DATE) <= papf.EFFECTIVE_END_DATE
        --papf.EMPLOYEE_NUMBER = '21162395'
    Order by
        EMPLOYEE_NUMBER,
        EFFECTIVE_DATE,
        CLASSIFICATION_NAME,
        ELEMENT_NAME    
    """
    s_sql = s_sql.replace("%BEGIN%", year_start)
    s_sql = s_sql.replace("%END%", year_end)
    print(s_sql)

    # Execute the query
    try:
        funcoracle.oracle_sql_to_sqlite('People payroll', '000b_Table - oracle.csv', s_table_name, s_sql)
    except Exception as e:
        funcsys.ErrMessage(e)

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
    # s_sql = s_sql.replace("%PMONTH%",funcdate.prev_month())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Elements")

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
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
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
    if l_export:
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
    s_sql = s_sql.replace("%PYEARE%", funcdate.prev_yearend())
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
    if l_export:
        # Export the data
        print("Export previous packages...")
        sr_filet = sr_file
        sx_path = re_path + funcdate.prev_year() + "/"
        sx_file = "Payroll_001ax_package_"
        # sx_filet = sx_file + funcdate.prev_monthendfile()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
        funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """************************************************************************
    BALANCES CURRENT
    ************************************************************************"""
    print("---------- BALANCES CURRENT ----------")

    # BUILD THE PAY DEFINED BALANCES LIST
    print("Build defined balances list...")
    sr_file = "X000_PAY_DEFINED_BALANCES"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        db.DEFINED_BALANCE_ID,
        db.LEGISLATION_CODE,
        db.BALANCE_TYPE_ID,
        db.BALANCE_DIMENSION_ID,
        Max(db.OBJECT_VERSION_NUMBER) As Max_OBJECT_VERSION_NUMBER
    From
        PAY_DEFINED_BALANCES db
    Group By
        db.DEFINED_BALANCE_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE PAY BALANCE TYPE LIST
    print("Build balance type list list...")
    sr_file = "X000_PAY_BALANCE_TYPE"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        bt.BALANCE_TYPE_ID,
        bt.BALANCE_NAME,
        bt.REPORTING_NAME,
        bt.BALANCE_CATEGORY_ID,
        bt.BALANCE_UOM,
        Max(bt.OBJECT_VERSION_NUMBER) As Max_OBJECT_VERSION_NUMBER
    From
        PAY_BALANCE_TYPES bt
    Group By
        bt.BALANCE_TYPE_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Build the balances list ******************************************************
    print("Build the balances list...")
    sr_file = "X000aa_balance_list_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        rb.RUN_BALANCE_ID,
        rb.ASSIGNMENT_ID,
        rb.EFFECTIVE_DATE,
        Upper(bt.BALANCE_NAME) As BALANCE_NAME,
        Upper(bt.REPORTING_NAME) As REPORTING_NAME,
        rb.BALANCE_VALUE,
        db.DEFINED_BALANCE_ID,
        bt.BALANCE_TYPE_ID
    From
        PAY_RUN_BALANCES_CURR rb Left Join
        X000_PAY_DEFINED_BALANCES db On db.DEFINED_BALANCE_ID = rb.DEFINED_BALANCE_ID Left Join
        X000_PAY_BALANCE_TYPE bt On bt.BALANCE_TYPE_ID = db.BALANCE_TYPE_ID    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Balances")

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
    s_sql = s_sql.replace("%PMONTHEND%", funcdate.prev_monthend())
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
    if l_export:
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
    Select
        rb.RUN_BALANCE_ID,
        rb.ASSIGNMENT_ID,
        rb.EFFECTIVE_DATE,
        Upper(bt.BALANCE_NAME) As BALANCE_NAME,
        Upper(bt.REPORTING_NAME) As REPORTING_NAME,
        rb.BALANCE_VALUE,
        db.DEFINED_BALANCE_ID,
        bt.BALANCE_TYPE_ID
    From
        PAY_RUN_BALANCES_PREV rb Left Join
        X000_PAY_DEFINED_BALANCES db On db.DEFINED_BALANCE_ID = rb.DEFINED_BALANCE_ID Left Join
        X000_PAY_BALANCE_TYPE bt On bt.BALANCE_TYPE_ID = db.BALANCE_TYPE_ID    
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
    s_sql = s_sql.replace("%PYEARE%", funcdate.prev_yearend())
    # print(s_sql)
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
    if l_export:
        # Export the data
        print("Export previous incomes...")
        sr_filet = sr_file
        sx_path = re_path + funcdate.prev_year() + "/"
        sx_file = "Payroll_002ax_income_total_"
        # sx_filet = sx_file + funcdate.prev_monthendfile()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
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
    s_sql = s_sql.replace("%PYEARE%", funcdate.prev_yearend())
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
