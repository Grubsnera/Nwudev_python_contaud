Select
    X004aa_people_bank_acc_master.*,
    X004ab_id_duplicate_count.COUNT
From
    X004aa_people_bank_acc_master Left Join
    X004ab_id_duplicate_count On X004ab_id_duplicate_count.ACC_NUMBER = X004aa_people_bank_acc_master.ACC_NUMBER
Where
    X004ab_id_duplicate_count.COUNT > 1
