<?php

// Get the assignment search id
$record_hash = JFactory::getApplication()->input->getString('hash');
//$record_hash = '2208937f0a0086a24c1eaedf5503823c';

// Exit page if no hash tag was supplied
$record_hash_test = "-".$record_hash."-";
if ($record_hash_test == '--') {
	die("You are not authorised to view this page!");
}

// Build the query
$query = "
Select
    concat( assi.ia_assi_name, ' ', assi.ia_assi_auto) As ia_assi_name,
    concat( find.ia_find_name, ' ', find.ia_find_auto) As ia_find_name,
    assi.ia_assi_auto,
    find.ia_find_auto,
    reme.ia_findreme_mail_trigger,
    reme.ia_findreme_mail_message,
    reme.ia_findresp_auto,
    reme.ia_findreme_response,
    reme.ia_findreme_auditor,
    reme.ia_findreme_auditor_email,
    reme.ia_findreme_client_message,
    reme.ia_findresp_desc,
    reme.ia_findreme_formview,
    reme.ia_findreme_name,
    reme.ia_findreme_mail,
    reme.ia_findreme_date_open,
    reme.ia_findreme_date_submit,
    reme.ia_findrate_auto,
    rate.ia_findrate_desc,
    reme.ia_findlike_auto,
    hood.ia_findlike_desc,
    reme.ia_findcont_auto,
    cont.ia_findcont_desc
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_response resp On resp.ia_findresp_auto = reme.ia_findresp_auto Left Join
    ia_finding_rate rate On rate.ia_findrate_auto = reme.ia_findrate_auto Left Join
    ia_finding_likelihood hood On hood.ia_findlike_auto = reme.ia_findlike_auto Left Join
    ia_finding_control cont On cont.ia_findcont_auto = reme.ia_findcont_auto
Where
    reme.ia_findreme_token = '".$record_hash."'
";
// reme.ia_findreme_token = 'e47322b49c1bc2bb01245972ba65cdd8'
// reme.ia_findreme_token = '".$record_hash."'

// Open the foreign database
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

// Do a test to see if form is still valid
if ($result->ia_findreme_mail_trigger <> 2) {
	$mess = "This audit remediation request form is no longer valid or has been answered!\n";
	$mess .= "If you submitted the form before you finished it, please contact the auditor to re-open the remediation request.\n";
	$mess .= "Thank you.\n";
	$mess .= "Internal Audit";
	die($mess);
}

// Populate the form fields with data read from the table
$val['assignment_auto'] = $result->ia_assi_auto;
$val['assignment'] = $result->ia_assi_name;
$val['finding_auto'] = $result->ia_find_auto;
$val['finding'] = $result->ia_find_name;
$val['message_from'] = $result->ia_findreme_mail_message;
$val['trigger'] = $result->ia_findreme_mail_trigger;
$val['response_indicator'] = $result->ia_findresp_auto;
$val['response'] = $result->ia_findreme_response;
$val['auditor'] = $result->ia_findreme_auditor;
$val['auditor_email'] = $result->ia_findreme_auditor_email;
$val['message_to'] = $result->ia_findreme_client_message;
$val['response_text'] = $result->ia_findresp_desc;
$val['client'] = $result->ia_findreme_name;
$val['client_email'] = $result->ia_findreme_mail;
$val['date_open'] = $result->ia_findreme_date_open;
$val['date_send'] = $result->ia_findreme_date_submit;
$val['impact'] = $result->ia_findrate_auto;
$val['impact_desc'] = $result->ia_findrate_desc;
$val['likelihood'] = $result->ia_findlike_auto;
$val['likelihood_desc'] = $result->ia_findlike_desc;
$val['control'] = $result->ia_findcont_auto;
$val['control_desc'] = $result->ia_findcont_desc;
$formviewlink = "https://nwu-ia.co.za/index.php?option=com_content&view=article&id=";
$formviewlink .= $result->ia_findreme_formview;
$formviewlink .= "&hash=".$record_hash;

?>