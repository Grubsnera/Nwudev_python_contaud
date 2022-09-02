﻿Select
    assi.ia_assi_auto,
    assi.ia_assi_token,
    assi.ia_user_sysid,
    assi.ia_assi_permission,
    assi.ia_assi_name,
    assi.ia_assi_year,
    assi.ia_assi_period,
    assi.ia_assi_startdate,
    assi.ia_assi_completedate,
    type.ia_assitype_file,
    assi.ia_assicond_auto,
    assi.ia_assitype_auto,
    assi.ia_assiorig_auto,
    assi.ia_assisite_auto,
    assi.ia_assicate_auto,
    assi.ia_assirepo_auto,
    assi.ia_assistat_auto,
    assi.ia_assi_priority,
    assi.ia_assi_desc,
    assi.ia_assi_offi,
    assi.ia_assi_report_toggle,
    assi.ia_assi_report,
    assi.ia_assi_proofdate,
    assi.ia_assi_finishdate,
    assi.ia_assi_si_reportdate,
    assi.ia_assi_si_report1days,
    assi.ia_assi_si_report1date,
    assi.ia_assi_si_report2days,
    assi.ia_assi_si_report2date,
    assi.ia_assi_si_caseyear,
    assi.ia_assi_si_casenumber,
    assi.ia_assi_si_boxyear,
    assi.ia_assi_si_boxnumber,
    assi.ia_assi_si_accused,
    assi.ia_assi_si_issue,
    assi.ia_assi_si_reference,
    assi.ia_assi_si_value,
    assi.ia_assi_header_toggle,
    assi.ia_assi_header,
    assi.ia_assi_footer_toggle,
    assi.ia_assi_footer,
    assi.ia_assi_signature_toggle,
    assi.ia_assi_signature
From
    ia_assignment assi Left Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto
Where
    assi.ia_assi_auto = 521 And
    assi.ia_assi_token = '2dd2f49722f31a5a63baaa84883b9ecb'