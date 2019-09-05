Select
    X002ab_vss_transort.*,
    X002ab_vss_transort.STUDENT_VSS As STUDENT_VSS1,
    X002ab_vss_transort.TRANSCODE_VSS As TRANSCODE_VSS1
From
    X002ab_vss_transort
Where
    X002ab_vss_transort.STUDENT_VSS = '21573557' And
    X002ab_vss_transort.TRANSCODE_VSS = '905'