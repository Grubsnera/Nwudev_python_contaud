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
$val['action_selector'] = $action;
$val['customer'] = $_SESSION['customer_id'];

// Fill the form when edit or copy
if ($id > 0) {

// Build the query
$query = "
Select
    reme.ia_findreme_auto,
    reme.ia_findreme_token,
    assi.ia_assi_auto,
    find.ia_find_auto,
    concat('(', assi.ia_assi_auto, ') ', assi.ia_assi_name) As ia_assi_name,
    concat('(', find.ia_find_auto, ') ', find.ia_find_name) As ia_find_name,
    reme.ia_findreme_auditor,
    reme.ia_findreme_auditor_email,
    reme.ia_findreme_employee,
    reme.ia_findreme_name,
    reme.ia_findreme_mail,
    reme.ia_findreme_mail_message,
    reme.ia_findreme_mail_trigger,
    reme.ia_findreme_client_message,
    reme.ia_findresp_auto,
    reme.ia_findresp_desc,
    reme.ia_findreme_response,
    reme.ia_findreme_formview,
    reme.ia_findreme_token,
    reme.ia_findreme_formclient,
    find.ia_find_desc,
    find.ia_find_condition,
    find.ia_find_effect,
    find.ia_find_cause,
    find.ia_find_recommend,
    reme.ia_findreme_date_send,
    reme.ia_findreme_date_open,
    reme.ia_findreme_date_submit,
    reme.ia_findreme_date_schedule,
    reme.ia_findreme_date_update,
    reme.ia_findreme_schedule,
    Concat(clra.ia_findrate_name, ' (', clra.ia_findrate_impact, ') - ', clra.ia_findrate_desc) As ia_findrate_name,
    Concat(clli.ia_findlike_name, ' (', clli.ia_findlike_value, ') - ', clli.ia_findlike_desc) As ia_findlike_name,
    Concat(clco.ia_findcont_name, ' (', clco.ia_findcont_value, ') - ', clco.ia_findcont_desc) As ia_findcont_name,
    reme.ia_findreme_attach,
    ia_user.ia_user_position,
	assi.ia_assi_customer
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_response resp On resp.ia_findresp_auto = reme.ia_findresp_auto Left Join
    ia_finding_rate clra On clra.ia_findrate_auto = reme.ia_findrate_auto Left Join
    ia_finding_likelihood clli On clli.ia_findlike_auto = reme.ia_findlike_auto Left Join
    ia_finding_control clco On clco.ia_findcont_auto = reme.ia_findcont_auto Left Join
    ia_user On ia_user.ia_user_sysid = assi.ia_user_sysid
Where
   reme.ia_findreme_auto = '".$id."' and
   reme.ia_findreme_token = '".$hash."'
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

// Populate the form fields with data read from the table.
if ($action == 'copy') {

	// Form copy
	//$val['finding'] = $result->ia_find_name.' - COPY';

} else {
	
	// Form edit and delete
	
		$val['status'] = $result->ia_findreme_mail_trigger;
		$val['schedule'] = $result->ia_findreme_schedule;
		$val['schedule_date'] = $result->ia_findreme_date_schedule;
		$val['date_send'] = $result->ia_findreme_date_send;
		$val['date_client_open'] = $result->ia_findreme_date_open;
		$val['date_client_send'] = $result->ia_findreme_date_submit;
		$val['date_update'] = $result->ia_findreme_date_update;
		$val['message_from'] = $result->ia_findreme_client_message;
		$val['response_indicator'] = $result->ia_findresp_auto;
		$val['response_text'] = $result->ia_findresp_desc;
		$val['impact_client'] = $result->ia_findrate_name;
		$val['likelihood_client'] = $result->ia_findlike_name;
		$val['control_client'] = $result->ia_findcont_name;

		// Add title to response
		$response = $result->ia_findreme_response;
		// $response = $result->ia_findreme_name." replied on ".$result->ia_findreme_date_submit."\n\n";
		// $response .= $result->ia_findreme_response;
		$val['response'] = $response;

}

	// Include for all actions except add
	$val['assignment'] = $result->ia_assi_auto;
	$val['finding'] = $result->ia_find_auto;
	$val['name_auditor'] = $result->ia_findreme_auditor;
	$val['email_auditor'] = $result->ia_findreme_auditor_email;
	$val['auditor_position'] = $result->ia_user_position;
	$val['employee'] = $result->ia_findreme_employee;
	$val['name'] = $result->ia_findreme_name;
	$val['email'] = $result->ia_findreme_mail;
	$val['message'] = $result->ia_findreme_mail_message;

}

// Fill the form in all instances

// Article_view_link
$article_view_link = "https://www.nwu-ia.co.za/index.php?option=com_content&view=article&id=";
$article_view_link .= $result->ia_findreme_formview;
$article_view_link .= "&hash=";
$article_view_link .= $result->ia_findreme_token;
$val['article_view_link'] = $article_view_link;

// Form_client_edit_link
$form_client_edit_link = "https://www.nwu-ia.co.za/index.php?option=com_rsform&view=rsform&formId=";
$form_client_edit_link .= $result->ia_findreme_formclient;
$form_client_edit_link .= "&customer=";
$form_client_edit_link .= $result->ia_assi_customer;
$form_client_edit_link .= "&hash=";
$form_client_edit_link .= $result->ia_findreme_token;
$val['form_client_edit_link'] = $form_client_edit_link;

// Audit finding detail
$finding_detail = "";
if ($result->ia_find_desc == '' or $result->ia_find_desc == '<p> </p>') {
} else {
	$finding_detail .= $result->ia_find_desc."\n";
}
if ($result->ia_find_condition == '' or $result->ia_find_condition == '<p> </p>') {
} else {
	$finding_detail .= $result->ia_find_condition."\n";
}
if ($result->ia_find_effect == '' or $result->ia_find_effect == '<p> </p>') {
} else {
	$finding_detail .= $result->ia_find_effect."\n";
}
if ($result->ia_find_cause == '' or $result->ia_find_cause == '<p> </p>') {
} else {
	$finding_detail .= $result->ia_find_cause."\n";
}
if ($result->ia_find_recommend == '' or $result->ia_find_recommend == '<p> </p>') {
} else {
	$finding_detail .= $result->ia_find_recommend."\n";
}
$val['finding_detail'] = $finding_detail;

// Date example
//$val[''] = date('Y-m-d', strtotime($result->));

?>