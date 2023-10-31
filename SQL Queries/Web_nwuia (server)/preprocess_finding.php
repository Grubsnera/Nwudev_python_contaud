<?php

// Get the assignment search id
$id = JFactory::getApplication()->input->getString('recordId');
$hash = JFactory::getApplication()->input->getString('recordHash');
$action = JFactory::getApplication()->input->getString('action');

// Exit page if no id or hash was supplied
$id_test = "-".$id."-";
$hash_test = "-".$hash."-";
if ($id_test == '--' or $hash_test == '--') {
$mess = 'You are not authorised to view this page!';
$mess .= "Web Administrator";
die($mess);
}

// Populate the form fields with data read from the table
$val['id'] = $id;
$val['hash'] = $hash;
$val['action'] = $action;
$val['customer'] = $_SESSION['customer_id'];

// Fill the form
if ($id > 0) {
	
	// Fill the form when a record exist

	// Build the query
	$query = "
	Select
		find.ia_assi_auto,
		find.ia_find_name,
		find.ia_find_createdate,
		find.ia_find_private,
		find.ia_findstat_auto,
		find.ia_find_note,
		find.ia_find_desc_toggle,
		find.ia_find_desc,
		find.ia_find_risk_toggle,
		find.ia_find_risk,
		find.ia_find_criteria_toggle,
		find.ia_find_criteria,
		find.ia_find_procedure_toggle,
		find.ia_find_procedure,
		find.ia_find_condition_toggle,
		find.ia_find_condition,
		find.ia_find_effect_toggle,
		find.ia_find_effect,
		find.ia_find_cause_toggle,
		find.ia_find_cause,
		find.ia_find_recommend_toggle,
		find.ia_find_recommend,
		find.ia_find_comment_toggle,
		find.ia_find_comment,
		find.ia_find_frequency_toggle,
		find.ia_find_frequency,
		find.ia_find_definition_toggle,
		find.ia_find_definition,
		find.ia_find_reference_toggle,
		find.ia_find_reference,
		find.ia_find_riskmatrix_toggle,
		find.ia_findlike_auto,
		flik.ia_findlike_desc,
		find.ia_findrate_auto,
		frat.ia_findrate_desc,
		find.ia_findcont_auto,
		fcon.ia_findcont_desc,
		find.ia_findlike_auto_client,
		clik.ia_findlike_desc As ia_findlike_desc_client,
		find.ia_findrate_auto_client,
		crat.ia_findrate_desc As ia_findrate_desc_client,
		find.ia_findcont_auto_client,
		ccon.ia_findcont_desc As ia_findcont_desc_client
	From
		ia_finding find Left Join
		ia_finding_likelihood flik On flik.ia_findlike_auto = find.ia_findlike_auto Left Join
		ia_finding_rate frat On frat.ia_findrate_auto = find.ia_findrate_auto Left Join
		ia_finding_control fcon On fcon.ia_findcont_auto = find.ia_findcont_auto Left Join
		ia_finding_likelihood clik On clik.ia_findlike_auto = find.ia_findlike_auto_client Left Join
		ia_finding_rate crat On crat.ia_findrate_auto = find.ia_findrate_auto_client Left Join
		ia_finding_control ccon On ccon.ia_findcont_auto = find.ia_findcont_auto_client
	Where
		find.ia_find_auto = ".$id." And
		find.ia_find_token = '".$hash."'
	";

	// Open a local database
	$db = JFactory::getDbo();
	$db->setQuery($query);
	$results = $db->loadObjectList();

	// Do a test to see if form is still valid
	if (empty($results)) {
	$mess = "The database record no longer exist!\n";
	$mess .= "Web Administrator";
	die($mess);
	}

	// Get the result
	$result = $results[0];

	// Populate the form fields with data read from the table\
	if ($action == 'copy') {

	$val['finding'] = $result->ia_find_name.' - COPY';
	$val['riskmatrix_toggle'] = 'a';

	// Finding description
	if (empty($result->ia_assi_desc) == False) {
	$val['description_toggle'] = '1';
	$val['description'] = $result->ia_find_desc;
	}

	// Finding risks
	if (empty($result->ia_assi_risk) == False) {
	$val['risk_toggle'] = '1';
	$val['risk'] = $result->ia_find_risk;
	}

	// Finding criteria
	if (empty($result->ia_assi_criteria) == False) {
	$val['criteria_toggle'] = '1';
	$val['criteria'] = $result->ia_find_criteria;
	}

	// Finding procedure
	if (empty($result->ia_assi_procedure) == False) {
	$val['procedure_toggle'] = '1';
	$val['procedure'] = $result->ia_find_procedure;
	}

	// Finding condition
	if (empty($result->ia_assi_condition) == False) {
	$val['condition_toggle'] = '1';
	$val['condition'] = $result->ia_find_condition;
	}

	// Finding effect
	if (empty($result->ia_assi_effect) == False) {
	$val['effect_toggle'] = '1';
	$val['effect'] = $result->ia_find_effect;
	}

	// Finding cause
	if (empty($result->ia_assi_cause) == False) {
	$val['cause_toggle'] = '1';
	$val['cause'] = $result->ia_find_cause;
	}

	// Finding recommend
	if (empty($result->ia_assi_recommend) == False) {
	$val['recommend_toggle'] = '1';
	$val['recommend'] = $result->ia_find_recommend;
	}

	// Finding comment
	if (empty($result->ia_assi_comment) == False) {
	$val['comment_toggle'] = '1';
	$val['comment'] = $result->ia_find_comment;
	}

	// Finding frequency
	if (empty($result->ia_assi_frequency) == False) {
	$val['frequency_toggle'] = '1';
	$val['frequency'] = $result->ia_find_frequency;
	}

	// Finding definition
	if (empty($result->ia_assi_definition) == False) {
	$val['definition_toggle'] = '1';
	$val['definition'] = $result->ia_find_definition;
	}

	// Finding reference
	if (empty($result->ia_assi_reference) == False) {
	$val['reference_toggle'] = '1';
	$val['reference'] = $result->ia_find_reference;
	}

	} else {

	$val['finding'] = $result->ia_find_name;
	$val['status'] = $result->ia_findstat_auto;

	// Finding description
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_desc) and $result->ia_find_desc_toggle == '1') {
	$val['description_toggle'] = '0';
	} else {
	$val['description_toggle'] = $result->ia_find_desc_toggle;
	}
	$val['description'] = $result->ia_find_desc;

	// Finding risk
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_risk) and $result->ia_find_risk_toggle == '1') {
	$val['risk_toggle'] = '0';
	} else {
	$val['risk_toggle'] = $result->ia_find_risk_toggle;
	}
	$val['risk'] = $result->ia_find_risk;

	// Finding criteria
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_criteria) and $result->ia_find_criteria_toggle == '1') {
	$val['criteria_toggle'] = '0';
	} else {
	$val['criteria_toggle'] = $result->ia_find_criteria_toggle;
	}
	$val['criteria'] = $result->ia_find_criteria;

	// Finding procedure
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_procedure) and $result->ia_find_procedure_toggle == '1') {
	$val['procedure_toggle'] = '0';
	} else {
	$val['procedure_toggle'] = $result->ia_find_procedure_toggle;
	}
	$val['procedure'] = $result->ia_find_procedure;

	// Finding condition
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_condition) and $result->ia_find_condition_toggle == '1') {
	$val['condition_toggle'] = '0';
	} else {
	$val['condition_toggle'] = $result->ia_find_condition_toggle;
	}
	$val['condition'] = $result->ia_find_condition;

	// Finding effect
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_effect) and $result->ia_find_effect_toggle == '1') {
	$val['effect_toggle'] = '0';
	} else {
	$val['effect_toggle'] = $result->ia_find_effect_toggle;
	}
	$val['effect'] = $result->ia_find_effect;

	// Finding cause
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_cause) and $result->ia_find_cause_toggle == '1') {
	$val['cause_toggle'] = '0';
	} else {
	$val['cause_toggle'] = $result->ia_find_cause_toggle;
	}
	$val['cause'] = $result->ia_find_cause;

	// Finding recommend
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_recommend) and $result->ia_find_recommend_toggle == '1') {
	$val['recommend_toggle'] = '0';
	} else {
	$val['recommend_toggle'] = $result->ia_find_recommend_toggle;
	}
	$val['recommend'] = $result->ia_find_recommend;

	// Finding comment
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_comment) and $result->ia_find_comment_toggle == '1') {
	$val['comment_toggle'] = '0';
	} else {
	$val['comment_toggle'] = $result->ia_find_comment_toggle;
	}
	$val['comment'] = $result->ia_find_comment;

	// Finding frequency
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_frequency) and $result->ia_find_frequency_toggle == '1') {
	$val['frequency_toggle'] = '0';
	} else {
	$val['frequency_toggle'] = $result->ia_find_frequency_toggle;
	}
	$val['frequency'] = $result->ia_find_frequency;

	// Finding definition
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_definition) and $result->ia_find_definition_toggle == '1') {
	$val['definition_toggle'] = '0';
	} else {
	$val['definition_toggle'] = $result->ia_find_definition_toggle;
	}
	$val['definition'] = $result->ia_find_definition;

	// Finding reference
	// If there is nothing in the text field, then close the toggle
	if (empty($result->ia_find_reference) and $result->ia_find_reference_toggle == '1') {
	$val['reference_toggle'] = '0';
	} else {
	$val['reference_toggle'] = $result->ia_find_reference_toggle;
	}
	$val['reference'] = $result->ia_find_reference;

	// Risk matrix toggle
	$val['riskmatrix_toggle'] = $result->ia_find_riskmatrix_toggle;

	// Likelihood
	$val['likelihood'] = $result->ia_findlike_auto;
	$val['likelihood_description'] = $result->ia_findlike_desc;

	// Impact rating
	$val['impact'] = $result->ia_findrate_auto;
	$val['impact_description'] = $result->ia_findrate_desc;

	// Control effectiveness
	$val['control'] = $result->ia_findcont_auto;
	$val['control_description'] = $result->ia_findcont_desc;

	// Likelihood client
	$val['likelihood_client'] = $result->ia_findlike_auto_client;
	$val['likelihood_description_client'] = $result->ia_findlike_desc_client;

	// Impact rating client
	$val['impact_rating_client'] = $result->ia_findrate_auto_client;
	$val['impact_description_client'] = $result->ia_findrate_desc_client;

	// Control effectiveness client
	$val['control_client'] = $result->ia_findcont_auto_client;
	$val['control_description_client'] = $result->ia_findcont_desc_client;

	}

	// Include for all actions

	$val['create_date'] = date('Y-m-d', strtotime($result->ia_find_createdate));
	$val['note'] = $result->ia_find_note;
	$val['private'] = $result->ia_find_private;
	
} else {
	
	// Fill the form when there is no record

}

//$val[''] = date('Y-m-d', strtotime($result->));


?>