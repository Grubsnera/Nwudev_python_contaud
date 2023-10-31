<?php

// Get the assignment search id
$id = JFactory::getApplication()->input->getString('id');

// Exit page if no id tag was supplied
$id_test = "-".$id."-";
if ($id_test == '--') {
	die("You are not authorised to view this page!");
}

// Build the query
$query = "
Select
    reme.ia_findreme_auto,
    reme.ia_find_auto,
    reme.ia_findreme_response,
    reme.ia_findrate_auto,
    concat(frat.ia_findrate_name,' - ',frat.ia_findrate_desc) As ia_findrate_name,
    reme.ia_findlike_auto,
    concat(flik.ia_findlike_name,' - ',flik.ia_findlike_desc) As ia_findlike_name,
    reme.ia_findcont_auto,
    concat(fcon.ia_findcont_name,' - ',fcon.ia_findcont_desc) As ia_findcont_name,
    find.ia_find_comment,
    reme.ia_findreme_name,
    reme.ia_findreme_date_submit,
    reme.ia_findreme_mail_trigger
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_finding_likelihood flik On flik.ia_findlike_auto = reme.ia_findlike_auto Inner Join
    ia_finding_rate frat On frat.ia_findrate_auto = reme.ia_findrate_auto Inner Join
    ia_finding_control fcon On fcon.ia_findcont_auto = reme.ia_findcont_auto
Where
    reme.ia_findreme_auto = '".$id."'
";

// Open a local database
$iadb = JFactory::getDbo();
$iadb->setQuery($query);
$results = $iadb->loadObjectList();
	
// Do a test to see if form is still valid
if (empty($results)) {
	$mess = "This audit remediation request form is no longer exist!\n";
	$mess .= "If you submitted the form before you finished it, please contact the auditor to re-open the remediation request.\n";
	$mess .= "Thank you.\n";
	$mess .= "Internal Audit";
	die($mess);
}

// Get the result
$result = $results[0];

// Populate the form fields with data read from the table
$val['remediation_id'] = $result->ia_findreme_auto;
$val['finding_id'] = $result->ia_find_auto;
$val['likelihood'] = $result->ia_findlike_auto;
$val['likelihood_description'] = $result->ia_findlike_name;
$val['impact'] = $result->ia_findrate_auto;
$val['impact_description'] = $result->ia_findrate_name;
$val['control'] = $result->ia_findcont_auto;
$val['control_description'] = $result->ia_findcont_name;
// $val['remediation_trigger'] = $result->ia_findreme_mail_trigger; // By default set to zero

// Add title to response
// $response = $result->ia_find_comment."\n\n";
// $response .= $result->ia_findreme_name." replied on ".$result->ia_findreme_date_submit."\n\n";
$response = $result->ia_findreme_response;
$val['response'] = $response;


?>