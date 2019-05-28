""" Script to run MYSQL web lists
Created: 28 May 2019
Author: Albert J v Rensburg (NWU21162395)
"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE MYSQL DATABASES
BEGIN OF SCRIPT
OPEN THE PEOPLE DATABASES
EXPORT CURRENT PEOPLE
EXPORT CURRENT PEOPLE STRUCTURE
END OF SCRIPT
*****************************************************************************"""

def Mysql_lists():

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # Import python modules
    import sys

    # Add own module path
    sys.path.append('S:/_my_modules')

    # Import python objects
    import sqlite3    

    # Import own modules
    import funcfile
    import funcmysql

    # Script log file
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B005_MYSQL_LISTS")
    funcfile.writelog("-------------------------")
    print("-----------------")    
    print("B005_MYSQL_LISTS")
    print("-----------------")
    ilog_severity = 1
    
    # SQLITE Declare variables 
    s_sql = "" #SQL statements
    l_export = True
    l_mail = True
    l_vacuum = False

    """*****************************************************************************
    OPEN THE MYSQL DATABASES
    *****************************************************************************"""
    print("OPEN THE MYSQL DATABASES")
    funcfile.writelog("OPEN THE MYSQL DATABASES")

    # Open the MYSQL DESTINATION table
    s_database = "Web_ia_nwu"
    ms_cnxn = funcmysql.mysql_open(s_database)
    ms_curs = ms_cnxn.cursor()
    funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_database)

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """*****************************************************************************
    OPEN THE PEOPLE DATABASES
    *****************************************************************************"""
    print("OPEN THE PEOPLE DATABASES")
    funcfile.writelog("OPEN THE PEOPLE DATABASES")

    # Open the SQLITE SOURCE file
    so_path = "W:/People/" #Source database path
    so_file = "People.sqlite" #Source database
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("%t OPEN SQLITE DATABASE: PEOPLE.SQLITE")

    """*****************************************************************************
    EXPORT CURRENT PEOPLE
    *****************************************************************************"""
    print("EXPORT CURRENT PEOPLE")
    funcfile.writelog("EXPORT CURRENT PEOPLE")

    # EXPORT CURRENT PEOPLE TO MYSQL
    print("Build mysql current people...")
    ms_curs.execute("DROP TABLE IF EXISTS ia_people")
    funcfile.writelog("%t DROPPED MYSQL TABLE: PEOPLE (ia_people)")
    #ia_find_1_auto INT(11) NOT NULL AUTO_INCREMENT,
    s_sql = """
    CREATE TABLE IF NOT EXISTS ia_people (
    ia_find_auto INT(11) NOT NULL,
    people_employee_number VARCHAR(20),
    people_ass_id INT(11),
    people_person_id INT(11),
    people_ass_numb VARCHAR(30),
    people_party_id INT(11),
    people_full_name VARCHAR(150),
    people_name_list VARCHAR(150),
    people_name_addr VARCHAR(150),    
    people_known_name VARCHAR(150),
    people_position_full VARCHAR(150),
    people_date_of_birth DATETIME,
    people_nationality VARCHAR(30),
    people_nationality_name VARCHAR(150),
    people_idno VARCHAR(150),
    people_passport VARCHAR(150),
    people_permit VARCHAR(150),
    people_tax_number VARCHAR(150),
    people_sex VARCHAR(30),
    people_marital_status VARCHAR(30),
    people_disabled VARCHAR(30),
    people_race_code VARCHAR(30),
    people_race_desc VARCHAR(150),
    people_lang_code VARCHAR(30),
    people_lang_desc VARCHAR(150),
    people_int_mail VARCHAR(30),
    people_email_address VARCHAR(150),
    people_curr_empl_flag VARCHAR(1),
    people_user_person_type VARCHAR(30),
    people_ass_start DATETIME,
    people_ass_end DATETIME,
    people_emp_start DATETIME,
    people_emp_end DATETIME,
    people_leaving_reason VARCHAR(30),
    people_leave_reason_descrip VARCHAR(150),
    people_location_description VARCHAR(150),
    people_org_type_desc VARCHAR(150),
    people_oe_code VARCHAR(30),
    people_org_name VARCHAR(150),
    people_primary_flag VARCHAR(1),
    people_acad_supp VARCHAR(30),
    people_faculty VARCHAR(150),
    people_division VARCHAR(150),
    people_employment_category VARCHAR(150),
    people_ass_week_len INT(2),
    people_leave_code VARCHAR(30),
    people_grade VARCHAR(30),
    people_grade_name VARCHAR(150),
    people_grade_calc VARCHAR(30),
    people_position VARCHAR(30),
    people_position_name VARCHAR(150),
    people_job_name VARCHAR(150),
    people_job_segment_name VARCHAR(150),
    people_supervisor INT(11),
    people_title_full VARCHAR(30),
    people_first_name VARCHAR(150),
    people_middle_names VARCHAR(150),
    people_last_name VARCHAR(150),
    people_phone_work VARCHAR(30),
    people_phone_mobi VARCHAR(30),
    people_phone_home VARCHAR(30),
    people_address_sars VARCHAR(250),
    people_address_post VARCHAR(250),
    people_address_home VARCHAR(250),
    people_address_othe VARCHAR(250),
    people_count_pos INT(11),
    people_count_ass INT(11),
    people_count_peo INT(11),
    people_date_ass_lookup DATETIME,
    people_ass_active VARCHAR(1),
    people_date_emp_lookup DATETIME,
    people_emp_active VARCHAR(1),
    people_mailto VARCHAR(150),
    people_proposed_salary_n VARCHAR(30),
    people_person_type VARCHAR(150),
    people_acc_type VARCHAR(30),
    people_acc_branch VARCHAR(30),
    people_acc_number VARCHAR(30),
    people_acc_relation VARCHAR(30),
    people_initials VARCHAR(30),
    people_age INT(3),
    people_month INT(2),
    people_day INT(2),
    PRIMARY KEY (people_employee_number),
    INDEX fb_order_people_full_name_INDEX (people_full_name),
    INDEX fb_order_people_known_name_INDEX (people_known_name)
    )
    ENGINE = InnoDB
    CHARSET=utf8mb4
    COLLATE utf8mb4_unicode_ci
    COMMENT = 'Table to store deatiled people data'
    """ + ";"
    ms_curs.execute(s_sql)
    funcfile.writelog("%t CREATED MYSQL TABLE: PEOPLE (ia_people)")

    # Open the SOURCE file to obtain column headings
    print("Build mysql current people columns...")
    funcfile.writelog("%t OPEN DATABASE: People")
    s_head = funcmysql.get_colnames_sqlite_text(so_curs,"X002_PEOPLE_CURR","people_")
    s_head = "(`ia_find_auto`, " + s_head.rstrip(", ") + ")"
    #print(s_head)

    # Open the SOURCE file to obtain the data
    print("Insert mysql current people...")
    with sqlite3.connect(so_path+so_file) as rs_conn:
        rs_conn.row_factory = sqlite3.Row
    rs_curs = rs_conn.cursor()
    rs_curs.execute("SELECT * FROM X002_PEOPLE_CURR")
    rows = rs_curs.fetchall()
    i_tota = 0
    i_coun = 0
    for row in rows:
        s_data = "(1, "
        for member in row:
            if type(member) == str:
                s_data = s_data + "'" + member + "', "
            elif type(member) == int:
                s_data = s_data + str(member) + ", "
            else:
                s_data = s_data + "'', "
        s_data = s_data.rstrip(", ") + ")"
        #print(s_data)
        s_sql = "INSERT INTO `ia_people` " + s_head + " VALUES " + s_data + ";"
        ms_curs.execute(s_sql)
        i_tota = i_tota + 1
        i_coun = i_coun + 1
        if i_coun == 100:
            ms_cnxn.commit()
            i_coun = 0
            
    # Close the ROW Connection
    ms_cnxn.commit()
    rs_conn.close()
    print("Inserted " + str(i_tota) + " mysql current people...")
    funcfile.writelog("%t POPULATE MYSQL: " + str(i_tota) + " People records (ia_people)")

    # Update MYSQL PEOPLE TO WEB FINDING mail trigger ******************************
    print("Update mysql current people mail trigger...")
    s_sql = """
    UPDATE `ia_finding` SET
    `ia_find_updated` = '1',
    `ia_find_r1_send` = '0',
    `ia_find_updatedate` = now()
    WHERE `ia_finding`.`ia_find_auto` = 1
    """ + ";"
    ms_curs.execute(s_sql)
    ms_cnxn.commit()
    funcfile.writelog("%t UPDATED MYSQL TRIGGER: FINDING 1 (people current)")

    # Update MYSQL PEOPLE TO WEB FINDING mail trigger ******************************
    print("Update mysql current people summary mail trigger...")
    s_sql = """
    UPDATE `ia_finding` SET
    `ia_find_updated` = '1',
    `ia_find_r1_send` = '0',
    `ia_find_updatedate` = now()
    WHERE `ia_finding`.`ia_find_auto` = 2
    """ + ";"
    ms_curs.execute(s_sql)
    ms_cnxn.commit()
    funcfile.writelog("%t UPDATED MYSQL TRIGGER: FINDING 2 (people current summary)")

    # Update MYSQL PEOPLE TO WEB FINDING mail trigger ******************************
    print("Update mysql current people birthday mail trigger...")
    s_sql = """
    UPDATE `ia_finding` SET
    `ia_find_updated` = '1',
    `ia_find_r1_send` = '0',
    `ia_find_updatedate` = now()
    WHERE `ia_finding`.`ia_find_auto` = 3
    """ + ";"
    ms_curs.execute(s_sql)
    ms_cnxn.commit()
    funcfile.writelog("%t UPDATED MYSQL TRIGGER: FINDING 3 (people current birthdays)")

    """*****************************************************************************
    EXPORT CURRENT PEOPLE STRUCTURE
    *****************************************************************************"""
    print("EXPORT CURRENT PEOPLE STRUCTURE")
    funcfile.writelog("EXPORT CURRENT PEOPLE STRUCTURE")

    # EXPORT CURRENT PEOPLE STRUCTURE
    print("Build mysql current people structure...")
    ms_curs.execute("DROP TABLE IF EXISTS ia_people_struct")
    funcfile.writelog("%t DROPPED MYSQL TABLE: PEOPLE_STRUCT (ia_people_struct)")
    #ia_find_1_auto INT(11) NOT NULL AUTO_INCREMENT,
    s_sql = """
    CREATE TABLE IF NOT EXISTS ia_people_struct (
    ia_find_auto INT(11) NOT NULL,
    struct_employee_one VARCHAR(20),
    struct_name_list_one TEXT,
    struct_known_name_one TEXT,
    struct_position_full_one TEXT,
    struct_location_description_one TEXT,
    struct_division_one TEXT,
    struct_faculty_one TEXT,
    struct_email_address_one TEXT,
    struct_phone_work_one TEXT,
    struct_phone_mobi_one TEXT,
    struct_phone_home_one TEXT,
    struct_org_name_one TEXT,
    struct_grade_calc_one TEXT,
    struct_employee_two VARCHAR(20),
    struct_name_list_two TEXT,
    struct_known_name_two TEXT,
    struct_position_full_two TEXT,
    struct_location_description_two TEXT,
    struct_division_two TEXT,
    struct_faculty_two TEXT,
    struct_email_address_two TEXT,
    struct_phone_work_two TEXT,
    struct_phone_mobi_two TEXT,
    struct_phone_home_two TEXT,
    struct_employee_three VARCHAR(20),
    struct_name_list_three TEXT,
    struct_known_name_three TEXT,
    struct_position_full_three TEXT,
    struct_location_description_three TEXT,
    struct_division_three TEXT,
    struct_faculty_three TEXT,
    struct_email_address_three TEXT,
    struct_phone_work_three TEXT,
    struct_phone_mobi_three TEXT,
    struct_phone_home_three TEXT,
    PRIMARY KEY (struct_employee_one)
    )
    ENGINE = InnoDB
    CHARSET=utf8mb4
    COLLATE utf8mb4_unicode_ci
    COMMENT = 'Table to store detailed people structure data'
    """ + ";"
    ms_curs.execute(s_sql)
    funcfile.writelog("%t CREATED MYSQL TABLE: PEOPLE_STRUCT (ia_people_struct)")
    # Open the SOURCE file to obtain column headings
    print("Build mysql current people structure columns...")
    funcfile.writelog("%t OPEN DATABASE: People org structure")
    s_head = funcmysql.get_colnames_sqlite_text(so_curs,"X003_PEOPLE_ORGA_REF","struct_")
    s_head = "(`ia_find_auto`, " + s_head.rstrip(", ") + ")"
    #print(s_head)
    # Open the SOURCE file to obtain the data
    print("Insert mysql current people structure...")
    with sqlite3.connect(so_path+so_file) as rs_conn:
        rs_conn.row_factory = sqlite3.Row
    rs_curs = rs_conn.cursor()
    rs_curs.execute("SELECT * FROM X003_PEOPLE_ORGA_REF")
    rows = rs_curs.fetchall()
    i_tota = 0
    i_coun = 0
    for row in rows:
        s_data = "(4, "
        for member in row:
            if type(member) == str:
                s_data = s_data + "'" + member + "', "
            elif type(member) == int:
                s_data = s_data + str(member) + ", "
            else:
                s_data = s_data + "'', "
        s_data = s_data.rstrip(", ") + ")"
        #print(s_data)
        s_sql = "INSERT INTO `ia_people_struct` " + s_head + " VALUES " + s_data + ";"
        ms_curs.execute(s_sql)
        i_tota = i_tota + 1
        i_coun = i_coun + 1
        if i_coun == 100:
            #print(i_coun)
            ms_cnxn.commit()
            i_coun = 0
    # Close the ROW Connection
    ms_cnxn.commit()
    rs_conn.close()
    print("Inserted " + str(i_tota) + " mysql current people...")
    funcfile.writelog("%t POPULATE MYSQL: " + str(i_tota) + " PEOPLE_STRUCT records (ia_people_struct)")

    # Update MYSQL PEOPLE TO WEB FINDING mail trigger ******************************
    print("Update mysql current people hierarchy mail trigger...")
    s_sql = """
    UPDATE `ia_finding` SET
    `ia_find_updated` = '1',
    `ia_find_r1_send` = '0',
    `ia_find_updatedate` = now()
    WHERE `ia_finding`.`ia_find_auto` = 4
    """ + ";"
    ms_curs.execute(s_sql)
    ms_cnxn.commit()
    funcfile.writelog("%t UPDATED MYSQL TRIGGER: FINDING 4 (people current hierarchy)")

    # CLOSE PEOPLE DATABASE
    so_conn.commit()
    so_conn.close()

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE MYSQL DATABASES
    ms_cnxn.commit()
    ms_cnxn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("---------------------------")
    funcfile.writelog("COMPLETED: B005_MYSQL_LISTS")

    return
