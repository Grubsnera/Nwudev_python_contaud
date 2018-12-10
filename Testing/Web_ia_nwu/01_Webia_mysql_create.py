"""
Script to test various MYSQL Functions
Created on: 12 Oct 2018
Created by: Albert J v Rensburg (21162395)
Modified on:
"""

import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import python objects
import pyodbc

# Define Functions
import funcfile

# Declare variables
s_sql = "" #SQL statements

# Script log file
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: WEB_IA_NWU")
funcfile.writelog("------------------")

# Connect to the oracle database
cnxn = pyodbc.connect("DSN=Web_ia_nwu; PWD=+C8+amXnmdo;")
curs = cnxn.cursor()
funcfile.writelog("%t OPEN DATABASE: Web-ia-nwu")

# Create ASSIGNMENT table
s_sql = "CREATE TABLE IF NOT EXISTS ia_assignment_test (" + """
ia_assi_auto INT(11) PRIMARY KEY AUTO_INCREMENT,
ia_assi_loaddate DATE,
ia_assi_year INT(4),
ia_assitype_auto INT(11),
ia_assiorig_auto INT(11),
ia_assisite_auto INT(11),
ia_assicate_auto INT(11),
ia_assirepo_auto INT(11),
ia_assistat_auto INT(11),
ia_user_name INT(11),
ia_assi_name VARCHAR(100),
ia_assi_priority TINYINT(1),
ia_assi_startdate DATE,
ia_assi_completedate DATE,
ia_assi_findingdate DATE,
ia_assi_proofdate DATE,
ia_assi_commentdate DATE,
ia_assi_finishdate DATE,
ia_assi_createdate DATETIME,
ia_assi_createby VARCHAR(50),
ia_assi_editdate DATETIME,
ia_assi_editby VARCHAR(50)
""" + ");"
curs.execute("DROP TABLE IF EXISTS ia_assignment_test")
curs.execute(s_sql)
cnxn.commit()
funcfile.writelog("%t CREATE TABLE: ia_assignment")

# Create ASSIGNMENT CATEGORY table
s_sql = "CREATE TABLE IF NOT EXISTS ia_assignment_category_test (" + """
ia_assicate_auto INT(11) NOT NULL AUTO_INCREMENT,
ia_assicate_name VARCHAR(50),
ia_assicate_desc TEXT,
ia_assicate_active TINYINT(1),
ia_assicate_private TINYINT(1),
ia_assicate_from DATE,
ia_assicate_to DATE,
ia_assicate_createdate DATETIME,
ia_assicate_createby VARCHAR(50),
ia_assicate_editdate DATETIME,
ia_assicate_editby VARCHAR(50),
PRIMARY KEY (ia_assicate_auto),
INDEX fb_order_ia_assicate_name_INDEX (ia_assicate_name),
INDEX fb_order_ia_assicate_from_INDEX (ia_assicate_from)
""" + ");"
curs.execute("DROP TABLE IF EXISTS ia_assignment_category_test")
curs.execute(s_sql)
cnxn.commit()
funcfile.writelog("%t CREATE TABLE: ia_assignment_category")

# Create ASSIGNMENT STATUS table
s_sql = "CREATE TABLE IF NOT EXISTS ia_assignment_status_test (" + """
ia_assistat_auto INT(11) NOT NULL AUTO_INCREMENT,
ia_assicate_auto INT(11) NOT NULL,
ia_assistat_name VARCHAR(50),
ia_assistat_desc TEXT,
ia_assistat_active TINYINT(1),
ia_assistat_from DATE,
ia_assistat_to DATE,
ia_assistat_createdate DATETIME,
ia_assistat_createby VARCHAR(50),
ia_assistat_editdate DATETIME,
ia_assistat_editby VARCHAR(50),
PRIMARY KEY (ia_assistat_auto),
INDEX fb_groupby_ia_assicate_auto_INDEX (ia_assicate_auto)
""" + ");"
curs.execute("DROP TABLE IF EXISTS ia_assignment_status_test")
curs.execute(s_sql)
cnxn.commit()
funcfile.writelog("%t CREATE TABLE: ia_assignment_status")

# Create ASSIGNMENT TYPE table
s_sql = "CREATE TABLE IF NOT EXISTS ia_assignment_type_test (" + """
ia_assitype_auto INT(11) NOT NULL AUTO_INCREMENT,
ia_assicate_auto INT(11) NOT NULL,
ia_assitype_file VARCHAR(20),
ia_assitype_name VARCHAR(50),
ia_assitype_desc TEXT,
ia_assitype_active TINYINT(1),
ia_assitype_from DATE,
ia_assitype_to DATE,
ia_assitype_createdate DATETIME,
ia_assitype_createby VARCHAR(50),
ia_assitype_editdate DATETIME,
ia_assitype_editby VARCHAR(50),
PRIMARY KEY (ia_assitype_auto),
INDEX fb_groupby_ia_assicate_auto_INDEX (ia_assicate_auto)
""" + ");"
curs.execute("DROP TABLE IF EXISTS ia_assignment_type_test")
curs.execute(s_sql)
cnxn.commit()
funcfile.writelog("%t CREATE TABLE: ia_assignment_type")

# Create ASSIGNMENT ORIGIN table
s_sql = "CREATE TABLE IF NOT EXISTS ia_assignment_origin_test (" + """
ia_assiorig_auto INT(11) NOT NULL AUTO_INCREMENT,
ia_assiorig_name VARCHAR(50),
ia_assiorig_desc TEXT,
ia_assiorig_active TINYINT(1),
ia_assiorig_from DATE,
ia_assiorig_to DATE,
ia_assiorig_createdate DATETIME,
ia_assiorig_createby VARCHAR(50),
ia_assiorig_editdate DATETIME,
ia_assiorig_editby VARCHAR(50),
PRIMARY KEY (ia_assiorig_auto),
INDEX fb_order_ia_assiorig_name_INDEX (ia_assiorig_name),
INDEX fb_order_ia_assiorig_from_INDEX (ia_assiorig_from)
""" + ");"
curs.execute("DROP TABLE IF EXISTS ia_assignment_origin_test")
curs.execute(s_sql)
cnxn.commit()
funcfile.writelog("%t CREATE TABLE: ia_assignment_origin")

# Create ASSIGNMENT REPORT table
s_sql = "CREATE TABLE IF NOT EXISTS ia_assignment_report_test (" + """
ia_assirepo_auto INT(11) NOT NULL AUTO_INCREMENT,
ia_assirepo_name VARCHAR(50),
ia_assirepo_desc TEXT,
ia_assirepo_active TINYINT(1),
ia_assirepo_from DATE,
ia_assirepo_to DATE,
ia_assirepo_createdate DATETIME,
ia_assirepo_createby VARCHAR(50),
ia_assirepo_editdate DATETIME,
ia_assirepo_editby VARCHAR(50),
PRIMARY KEY (ia_assirepo_auto),
INDEX fb_order_ia_assirepo_name_INDEX (ia_assirepo_name),
INDEX fb_order_ia_assirepo_from_INDEX (ia_assirepo_from)
""" + ");"
curs.execute("DROP TABLE IF EXISTS ia_assignment_report_test")
curs.execute(s_sql)
cnxn.commit()
funcfile.writelog("%t CREATE TABLE: ia_assignment_report")

# Create ASSIGNMENT SITE table
s_sql = "CREATE TABLE IF NOT EXISTS ia_assignment_site_test (" + """
ia_assisite_auto INT(11) NOT NULL AUTO_INCREMENT,
ia_assisite_name VARCHAR(50),
ia_assisite_desc TEXT,
ia_assisite_active TINYINT(1),
ia_assisite_from DATE,
ia_assisite_to DATE,
ia_assisite_createdate DATETIME,
ia_assisite_createby VARCHAR(50),
ia_assisite_editdate DATETIME,
ia_assisite_editby VARCHAR(50),
PRIMARY KEY (ia_assisite_auto),
INDEX fb_order_ia_assisite_name_INDEX (ia_assisite_name),
INDEX fb_order_ia_assisite_from_INDEX (ia_assisite_from)
""" + ");"
curs.execute("DROP TABLE IF EXISTS ia_assignment_report_test")
curs.execute(s_sql)
cnxn.commit()
funcfile.writelog("%t CREATE TABLE: ia_assignment_report")

# Create USER table
s_sql = "CREATE TABLE IF NOT EXISTS ia_user_test (" + """
ia_user_id INT(11) NOT NULL AUTO_INCREMENT,
ia_user_sysid INT(11) NOT NULL,
ia_user_name VARCHAR(50),
ia_user_position VARCHAR(50),
ia_user_active TINYINT(1),
ia_user_from DATE,
ia_user_to DATE,
PRIMARY KEY (ia_user_id),
INDEX fb_order_ia_user_name_INDEX (ia_user_name),
INDEX fb_order_ia_user_from_INDEX (ia_user_from)
""" + ");"
curs.execute("DROP TABLE IF EXISTS ia_user_test")
curs.execute(s_sql)
cnxn.commit()
funcfile.writelog("%t CREATE TABLE: ia_user")

# Create test table
s_sql = "CREATE TABLE IF NOT EXISTS `Ia_nwu`.`test` (" + """
`test1` INT NOT NULL AUTO_INCREMENT ,
`test2` INT NOT NULL ,
PRIMARY KEY  (`test1`),
INDEX  `test2index` (`test2`))
ENGINE = InnoDB
CHARSET=utf8mb4
COLLATE utf8mb4_unicode_ci
""" + ";"
curs.execute("DROP TABLE IF EXISTS test")
curs.execute(s_sql)
cnxn.commit()
funcfile.writelog("%t CREATE TABLE: test")


# Script log file
funcfile.writelog("---------------------")
funcfile.writelog("COMPLETED: WEB_IA_NWU")

