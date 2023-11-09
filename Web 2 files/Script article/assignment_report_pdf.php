{source}<?php

// Get the assignment search id
$hash = JFactory::getApplication()->input->getString('hash');
// $hash = 'bba4bc84d1c8b5f7b8b290f19c208eba';

// Exit page if no hash tag was supplied
$hash_test = "-".$hash."-";
if ($hash_test == '--') {
	die("You are not authorised to view this page!");
}

// Build the query
$query = "
Select
	assi.ia_assi_auto,
	assi.ia_assi_name,
	assi.ia_assi_report,
	assi.ia_assi_report_text1,
	assi.ia_assi_report_text2,
	assi.ia_assi_header,
	assi.ia_assi_header_text,
	assi.ia_assi_footer,
	assi.ia_assi_signature,
	assi.ia_assi_year,
	assi.ia_assi_file
From
	ia_assignment assi
Where
	assi.ia_assi_token = '".$hash."'
";

// Open a local database
$iadb = JFactory::getDbo();
$iadb->setQuery($query);
$results = $iadb->loadObjectList();

// Do a test to see if form is still valid
if (empty($results)) {
	$mess = "The audit assignment link is no longer valid!\n";
	$mess .= "Please contact the system administrator.\n";
	$mess .= "Thank you.\n";
	$mess .= "Internal Audit";
	die($mess);
}

// Display the assignment detail
$result = $results[0];
$finding_id = $result->ia_assi_auto;
$report_output = '';
$report_name = '';

// Assignment header title
if ($result->ia_assi_header_text == '' or $result->ia_assi_header_text == '<p> </p>') {
	$report_output .= '<h1>Audit report</h1>';
	$report_name = 'Audit report';
} else {
	$report_output .= '<h1>'.$result->ia_assi_header_text.'</h1>';
	$report_name = $result->ia_assi_header_text;
}
		
// Assignment header
if (!empty($result->ia_assi_header) && $result->ia_assi_header != '<p> </p>') {
	$report_output .= $result->ia_assi_header;
} 
		
// Assignment report
if (!empty($result->ia_assi_report) && $result->ia_assi_report != '<p> </p>') {
	$report_output .= $result->ia_assi_report;
} 
		
// Assignment footer
if (!empty($result->ia_assi_footer) && $result->ia_assi_footer != '<p> </p>') {
	$report_output .= $result->ia_assi_footer;
} 

// Get the findings related to the assignment
// Build the query
$query = "
Select
	assi.ia_assi_auto,
	assi.ia_assi_token,
	find.ia_find_auto,
	find.ia_find_name,
	find.ia_find_desc,
	find.ia_find_risk,
	find.ia_find_criteria,
	find.ia_find_procedure,
	find.ia_find_condition,
	find.ia_find_effect,
	find.ia_find_cause,
	find.ia_find_recommend,
	find.ia_find_comment,
	find.ia_find_frequency,
	find.ia_find_definition,
	find.ia_find_reference,
	find.ia_find_riskmatrix_toggle,
	rate.ia_findrate_name,
	rate.ia_findrate_desc,
	rate.ia_findrate_impact,
	lhoo.ia_findlike_name,
	lhoo.ia_findlike_desc,
	lhoo.ia_findlike_value,
	cont.ia_findcont_name,
	cont.ia_findcont_desc,
	cont.ia_findcont_value,
	clra.ia_findrate_name As ia_findrate_name_client,
	clra.ia_findrate_desc As ia_findrate_desc_client,
	clra.ia_findrate_impact As ia_findrate_impact_client,
	clli.ia_findlike_name As ia_findlike_name_client,
	clli.ia_findlike_desc As ia_findlike_desc_client,
	clli.ia_findlike_value As ia_findlike_value_client,
	clco.ia_findcont_name As ia_findcont_name_client,
	clco.ia_findcont_desc As ia_findcont_desc_client,
	clco.ia_findcont_value As ia_findcont_value_client
From
	ia_assignment assi Inner Join
	ia_finding find On find.ia_assi_auto = assi.ia_assi_auto Left join
	ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
	ia_finding_likelihood lhoo On lhoo.ia_findlike_auto = find.ia_findlike_auto Left Join
	ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto Left Join
	ia_finding_rate clra On clra.ia_findrate_auto = find.ia_findrate_auto_client Left Join
	ia_finding_likelihood clli On clli.ia_findlike_auto = find.ia_findlike_auto_client Left Join
	ia_finding_control clco On clco.ia_findcont_auto = find.ia_findcont_auto_client
Where
	assi.ia_assi_auto = '".$finding_id."' And
	find.ia_find_private = 0
Order By
	find.ia_find_name
";

// Open a local database
$finddb = JFactory::getDbo();
$finddb->setQuery($query);
$findings = $finddb->loadObjectList();


if (count($findings) > 1) {
	
	// Finding header title
	if ($result->ia_assi_report_text2 == '' or $result->ia_assi_report_text2 == '<p> </p>') {
		$report_output .= '<h1>Audit findings</h1>';
	} else {
		$report_output .= '<h1>'.$result->ia_assi_report_text2.'</h1>';
	}

} elseif (count($findings) > 0) {
	
	if ($result->ia_assi_report_text1 == '' or $result->ia_assi_report_text1 == '<p> </p>') {
		$report_output .= '<h1>Audit finding</h1>';
	} else {
		$report_output .= '<h1>'.$result->ia_assi_report_text1.'</h1>';
	}

}

// Do only if findings exist
if (empty($findings)) {
} else {

	// Findings content - used later in full reports
	foreach ($findings as $finding) {

		//$report_output .= $finding->ia_find_auto.'<br>';
		//$report_output .= $finding->ia_find_name.'<br>';

		// Finding description
		if (!empty($finding->ia_find_desc) && $finding->ia_find_desc != '<p> </p>') {
			$report_output .= $finding->ia_find_desc;
		} 
		
		// Finding risk
		if (!empty($finding->ia_find_risk) && $finding->ia_find_risk != '<p> </p>') {
			$report_output .= $finding->ia_find_risk;
		} 

		// Audit evaluation of the effectiveness of controls
		if ($finding->ia_find_riskmatrix_toggle == 'a' or $finding->ia_find_riskmatrix_toggle == 'b') {
			
			if ($finding->ia_findrate_impact > 0 or $finding->ia_findlike_value > 0 or $finding->ia_findcont_value > 0) {
				$report_output .= "<h2>Audit evaluation of the effectiveness of controls</h2>";
			}
			if ($finding->ia_findrate_impact > 0) {
				$report_output .= "<strong>Impact rating</strong> - ";
				$report_output .= $finding->ia_findrate_name." (".$finding->ia_findrate_impact.")<br>";
			}
			if ($finding->ia_findrate_impact > 0) {
				$report_output .= $finding->ia_findrate_desc."<br><br>";
			}
			if ($finding->ia_findlike_value > 0) {
				$report_output .= "<strong>Likelihood</strong> - ";
				$report_output .= $finding->ia_findlike_name." (".$finding->ia_findlike_value.")<br>";
			}
			if ($finding->ia_findlike_value > 0) {
				$report_output .= $finding->ia_findlike_desc."<br><br>";
			}
			if ($finding->ia_findcont_value > 0) {
				$report_output .= "<strong>Effectiveness</strong> - ";
				$report_output .= $finding->ia_findcont_name." (".$finding->ia_findcont_value.")<br>";
			}
			if ($finding->ia_findcont_value > 0) {
				$report_output .= $finding->ia_findcont_desc."<br>";
			}
		
		}

		// Management evaluation of the effectiveness of controls
		if ($finding->ia_find_riskmatrix_toggle == 'm' or $finding->ia_find_riskmatrix_toggle == 'b') {
			
			if ($finding->ia_findrate_impact_client > 0 or $finding->ia_findlike_value_client > 0 or $finding->ia_findcont_value_client > 0) {
				$report_output .= "<h2>Management evaluation of the effectiveness of controls</h2>";
			}
			if ($finding->ia_findrate_impact_client > 0) {
				$report_output .= "<strong>Impact rating</strong> - ";
				$report_output .= $finding->ia_findrate_name." (".$finding->ia_findrate_impact_client.")<br>";
			}
			if ($finding->ia_findrate_impact_client > 0) {
				$report_output .= $finding->ia_findrate_desc."<br><br>";
			}
			if ($finding->ia_findlike_value_client > 0) {
				$report_output .= "<strong>Likelihood</strong> - ";
				$report_output .= $finding->ia_findlike_name." (".$finding->ia_findlike_value_client.")<br>";
			}
			if ($finding->ia_findlike_value_client > 0) {
				$report_output .= $finding->ia_findlike_desc."<br><br>";
			}
			if ($finding->ia_findcont_value_client > 0) {
				$report_output .= "<strong>Effectiveness</strong> - ";
				$report_output .= $finding->ia_findcont_name." (".$finding->ia_findcont_value_client.")<br>";
			}
			if ($finding->ia_findcont_value_client > 0) {
				$report_output .= $finding->ia_findcont_desc."<br>";
			}
		
		}
		
		// Finding criteria
		if (!empty($finding->ia_find_criteria) && $finding->ia_find_criteria != '<p> </p>') {
			$report_output .= $finding->ia_find_criteria;
		} 
		
		// Finding procedure
		if (!empty($finding->ia_find_procedure) && $finding->ia_find_procedure != '<p> </p>') {
			$report_output .= $finding->ia_find_procedure;
		} 
		
		// Finding condition
		if (!empty($finding->ia_find_condition) && $finding->ia_find_condition != '<p> </p>') {
			$report_output .= $finding->ia_find_condition;
		} 
		
		// Finding effect
		if (!empty($finding->ia_find_effect) && $finding->ia_find_effect != '<p> </p>') {
			$report_output .= $finding->ia_find_effect;
		} 

		// Finding cause
		if (!empty($finding->ia_find_cause) && $finding->ia_find_cause != '<p> </p>') {
			$report_output .= $finding->ia_find_cause;
		} 
		
		// Finding recommendation
		if (!empty($finding->ia_find_recommend) && $finding->ia_find_recommend != '<p> </p>') {
			$report_output .= $finding->ia_find_recommend;
		} 
		
		// Finding comment
		if (!empty($finding->ia_find_comment) && $finding->ia_find_comment != '<p> </p>') {
			$report_output .= $finding->ia_find_comment;
		} 
		
		// Finding frequency
		if (!empty($finding->ia_find_frequency) && $finding->ia_find_frequency != '<p> </p>') {
			$report_output .= $finding->ia_find_frequency;
		} 
		
		// Finding definition
		if (!empty($finding->ia_find_definition) && $finding->ia_find_definition != '<p> </p>') {
			$report_output .= $finding->ia_find_definition;
		} 
		
		// Finding reference
		if (!empty($finding->ia_find_reference) && $finding->ia_find_reference != '<p> </p>') {
			$report_output .= $finding->ia_find_reference;
		} 
	}
}
		
// Assignment signature
if (!empty($result->ia_assi_signature) && $result->ia_assi_signature != '<p> </p>') {
	$report_output .= $result->ia_assi_signature;
} 

// Assignment reference
$report_output .= '<br />';
$report_output .= '<p><span style="font-size: 8pt;">File reference: '.$result->ia_assi_year.'.'.$result->ia_assi_file.'</span></p>';

// Display the report
echo $report_output;

?>{/source}