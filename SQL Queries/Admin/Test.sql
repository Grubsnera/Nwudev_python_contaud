Select
    X001ad_auto_time."ACTION",
    Count(X001ad_auto_time.LOG_ELAPSED) As Count_LOG_ELAPSED,
    Total(X001ad_auto_time.LOG_SECOND) As Total_LOG_SECOND
From
    X001ad_auto_time
Group By
    X001ad_auto_time."ACTION"