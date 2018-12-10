SELECT
  X011_Studdeb_transort.CAMPUS AS gle02_CoaName,
  X011_Studdeb_transort.CAMPUS_VSS AS gle02_XCamp,
  X011_Studdeb_transort.MONTH AS gle09_YearId,
  X011_Studdeb_transort.AMOUNT AS gle15_XAmou,
  X011_Studdeb_transort.DATE_TRAN AS gle17_Date,
  X011_Studdeb_transort.STUDENT AS gle50_Stud,
  X011_Studdeb_transort.BURSARY AS gle51_Burs,
  X011_Studdeb_transort.DESC_FULL AS gle52_Desc,
  X011_Studdeb_transort.DESC_VSS AS gle53_Desc
FROM
  X011_Studdeb_transort
WHERE
  X011_Studdeb_transort.MONTH != 'BB'
