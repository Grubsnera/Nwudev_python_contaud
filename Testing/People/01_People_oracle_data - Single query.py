"""
Script to extract raw data from Oracle HR System
Created on: 14/12/2017
Created by: Albert J v Rensburg (21162395)
Modified on:
"""

# Declare variables
c_dest = r'W:\Oracle_hr\Export'

import csv
import pyodbc
cnxn = pyodbc.connect("DSN=Hr;PWD=potjiekos")
curs = cnxn.cursor()

c_sql = """
select 
papf.employee_number,
papf.full_name,
apps.xxnwu_oe_code(hro.organization_id,dec.creation_date) oe_code,
apps.xxnwu_oe_eng_name(hro.organization_id,dec.creation_date) organization,
apps.xxnwu_oe_responsible(hro.organization_id,dec.creation_date) oe_responsible,
dec.declaration_id,
  dec.understand_policy_flag,
  dec.interest_to_declare_flag,
  decstat.meaning declaration_status,
  dec.rejection_reason ,
  dec.full_disclosure_flag,
  dec.line_manager,
  dec.audit_user,
  dec.creation_date,
  dec.last_update_date,
  dec.last_updated_by,
  int.interest_id,
  int.entity_name,
  int.entity_registration_number,
  ind.description industry_classification,
  ind.meaning industry_classification_eng,
  intType.description interest_type,
  intType.meaning interest_type_eng,
  conType.description conflict_type ,
  conType.meaning conflict_type_eng,
  int.office_address,
  int.dir_appointment_date,
  int.perc_share_interest,
  int.description,
  int.task_perf_agreement,
  INT.STATUS_ID,
  int.mitigation_agreement ,
  int.line_manager line_manager_interest
 
from
apps.per_all_people_f papf,
apps.per_all_assignments_f paaf,
apps.hr_all_organization_units hro,
apps.xxnwu_coi_declarations DEC,
  apps.xxnwu_coi_interests       INT,
  apps.hr_lookups decstat,
  apps.hr_lookups ind,
  apps.hr_lookups intType,
  apps.hr_lookups conType,
  apps.hr_lookups stat
where papf.employee_number = dec.employee_number(+)
and dec.declaration_id   = int.declaration_id(+)
AND decstat.lookup_type(+) = 'NWU_COI_STATUS'
AND decstat.lookup_code(+) = dec.status_id
AND ind.lookup_type(+)     = 'NWU_COI_INDUSTRY_CLASS'
AND ind.lookup_code(+)     = int.industry_class_id
AND intType.lookup_type(+) = 'NWU_COI_INTEREST_TYPES'
AND intType.lookup_code(+) = int.interest_type_id
AND conType.lookup_type(+) = 'NWU_COI_CONFLICT_TYPE'
AND conType.lookup_code(+) = int.conflict_type_id
AND stat.lookup_type(+)    = 'NWU_COI_STATUS'
AND stat.lookup_code(+)    = int.status_id
and papf.person_id = paaf.person_id
and paaf.organization_id = hro.organization_id
and dec.creation_date between paaf.effective_start_date and paaf.effective_end_date
and dec.creation_date between papf.effective_start_date and papf.effective_end_date
"""

#and papf.employee_number = :employee_number
#and dec.declaration_date between :date_from and :date_to

# SINGLE QUERY
f = open(c_dest+"/QUERY.csv", "w")
csvf = csv.writer(f, lineterminator='\r', quoting=csv.QUOTE_NONNUMERIC)
#fnames = ['JOB_DEFINITION_ID', 'SEGMENT1']
#csvf.writerow(fnames)
for row in curs.execute(c_sql).fetchall():
    csvf.writerow(row)
f.close()



