Select
    X030ac_Trans_feemodu_mode.AMOUNT
From
    X031aa_Modu_nofee_loaded_modu Inner Join
    X030ac_Trans_feemodu_mode On X030ac_Trans_feemodu_mode.FMODAPID = X031aa_Modu_nofee_loaded_modu.FMODULEAPID
            And X030ac_Trans_feemodu_mode.ENROL_CAT = X031aa_Modu_nofee_loaded_modu.ENROL_CAT