Select
    tran.STUDENT As student,
    Total(tran.AMOUNT) As total_burs,
    Total(loan.AMOUNT) As total_loan,
    Total(inte.AMOUNT) As total_internal,
    Total(exte.AMOUNT) As total_external,
    Total(trus.AMOUNT) As total_trust    
From
    X000_Transaction tran Left Join
    X000_Transaction loan On loan.STUDENT = tran.STUDENT And loan.SOURCE = 'BURSARY-LOAN SCHEMA' Left Join
    X000_Transaction inte On inte.STUDENT = tran.STUDENT And inte.SOURCE = 'UNIVERSITY FUND' Left Join
    X000_Transaction exte On exte.STUDENT = tran.STUDENT And exte.SOURCE = 'EXTERNAL FUND' Left join
    X000_Transaction trus On trus.STUDENT = tran.STUDENT And trus.SOURCE = 'DONATE/TRUST FUND'
Group By
    tran.STUDENT