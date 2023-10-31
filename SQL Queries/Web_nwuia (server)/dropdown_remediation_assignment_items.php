<?php

//<code>

// Get the record id
$record_id = JFactory::getApplication()->input->getString('recordId');
//$record_id = 0;

// Get the record action
$action = JFactory::getApplication()->input->getString('action');
//$action = 'add';

// Get the assignment record id
$assignment_id = 0;
$assignment_id = JFactory::getApplication()->input->getString('assignment');

// Test to see if a super user or a customer
$user = JFactory::getUser();
$user_id = $user->get('id');
$customer_id = $_SESSION['customer_id'];

// Determine if the user is a super user = group = 8
$super_user = 0;
foreach ($user->getAuthorisedGroups() as $group) {
	if ($group == 8) {
		$super_user = 1;
	}
}

// Extract the data depending on the action
if ($customer_id == '' or $super_user > 0) {

	// echo 'No customer, proceed with function';

	// Build the query
	$query = "
	Select
		dtab.ia_assiorig_auto As value,
		case
		when dtab.ia_assiorig_active = 1 then concat(cont.name, ' - ', dtab.ia_assiorig_name, ' (Active) ', date(dtab.ia_assiorig_from), '/', date(dtab.ia_assiorig_to))
		else concat(cont.name, ' - ', dtab.ia_assiorig_name, ' (InActve) ', date(dtab.ia_assiorig_from), '/', date(dtab.ia_assiorig_to))
		end as label
	From
		ia_assignment_origin dtab Inner Join
		jm4_contact_details cont On cont.id = dtab.ia_assiorig_customer
	Order By
		label
	";

	// Add a please select
	$items[] = "|Please Select[c]";

} elseif ($action == 'add') {

	// echo 'Add, proceed with function';

	// Build the query
	$query = "
	Select
		assi.ia_assi_auto as value,
		Concat(cate.ia_assicate_name, ' (', assi.ia_assi_year, ') - ', assi.ia_assi_name, ' (', assi.ia_assi_auto, ') ', '(', type.ia_assitype_name, ')') As label
	From
		ia_assignment assi Inner Join
		ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
		ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Inner Join
		ia_finding find On find.ia_assi_auto = assi.ia_assi_auto Inner Join
		ia_finding_status On ia_finding_status.ia_findstat_auto = find.ia_findstat_auto
	Where
		(assi.ia_user_sysid = ".$user_id." And
		assi.ia_assi_priority < 9 And
		ia_finding_status.ia_findstat_name = 'Send for approval') Or
		(assi.ia_user_sysid = ".$user_id." And
		assi.ia_assi_priority < 9 And
		ia_finding_status.ia_findstat_name = 'Request remediation')
	Group By
		assi.ia_assi_auto,
		assi.ia_assi_name
	Order By
		cate.ia_assicate_name,
		assi.ia_assi_name
	";

	// Add a please select
	if ($assignment_id > 0) {
		$items[] = "|Please Select";
	} else {
		$items[] = "|Please Select[c]";
	}

} elseif ($action == 'edit' or $action == 'delete' or $action == 'copy') {

	// echo 'Customer, proceed with function';

	// Build the query
	$query = "
		Select
			assi.ia_assi_auto As value,
		Concat(cate.ia_assicate_name, ' (', assi.ia_assi_year, ') - ', assi.ia_assi_name, ' (', assi.ia_assi_auto, ') ', '(', type.ia_assitype_name, ')') As label
		From
			ia_assignment assi Inner Join
			ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
			ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto
		Where
			(assi.ia_user_sysid = ".$user_id." And
			assi.ia_assi_year = year(date_add(now(), interval -1 year))) Or
			(assi.ia_user_sysid = ".$user_id." And
			assi.ia_assi_year = year(now())) Or
			(assi.ia_user_sysid = ".$user_id." And
			assi.ia_assi_year = year(date_add(now(), interval 1 year)))
		Group by
			Concat(cate.ia_assicate_name, ' (', type.ia_assitype_name, ') ', assi.ia_assi_name, ' (', assi.ia_assi_auto, ')')
	";

} else {

	$items[] = '|Error (login first)[c]';

}

// Query the database
$db = JFactory::getDbo();
$db->setQuery($query);
$results = $db->loadObjectList();

// Format for RSForm! Pro dropdown format.
foreach ($results as $result) {
	if ($assignment_id > 0 and $assignment_id == $result->value) {
		$items[] = $result->value.'|'.$result->label.'[c]';
	} else {
		$items[] = $result->value.'|'.$result->label;
		// echo $result->value.'|'.$result->label.'<br/>';
	}
}

// Now we need to return the value to the field
return $items;

//</code>

?>