Select
    X000_Account.ACCOUNT_NBR,
    Count(X000_Account.VER_NBR) As Count_VER_NBR
From
    X000_Account
Group By
    X000_Account.ACCOUNT_NBR