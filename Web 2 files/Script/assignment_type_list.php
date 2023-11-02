<?php

// Script to obtain assignment status based on assignment category
// Returning the response name
// Search on response number
// 21 June 2023 Albert Janse van Rensburg

// Setup joomla to run an external php
define('_JEXEC', 1);    
use Joomla\CMS\Factory;
define('JPATH_BASE', realpath(dirname(__FILE__) . '/..'));
require_once JPATH_BASE . '/includes/defines.php';
require_once JPATH_BASE . '/includes/framework.php';
error_reporting(E_ALL);
ini_set('display_errors', 1);
$crlf = PHP_EOL;
$app = new Joomla\CMS\Application\SiteApplication();
\Joomla\CMS\Factory::$application = $app;

// Get the assignment search id
$record_category = "";
$record_category = JFactory::getApplication()->input->getString('category');
$customer_id = "";
$customer_id = JFactory::getApplication()->input->getString('customer');
$action = "";
$action = JFactory::getApplication()->input->getString('action');
$create_date = "";
$create_date = JFactory::getApplication()->input->getString('createdate');

// Report the parameters
// echo 'Category: '.$record_category;
// echo 'Customer: '.$customer_id;
// echo 'Action: '.$action;
// echo 'Create date: '.$create_date;

$customer = 0;

// Build the queries
if ($customer_id > 0) {
	
	if ($action == 'add') {
		
		// Add
		
		// Scripts to display normal user lists
		$query = "
		Select
			dtab.ia_assitype_auto As value,
			dtab.ia_assitype_name As label
		From
			ia_assignment_type dtab
		Where
			dtab.ia_assitype_active = 1 And
			dtab.ia_assitype_customer = ".$customer_id." And
			dtab.ia_assicate_auto = ".$record_category." And
			dtab.ia_assitype_from <= '".date('Y-m-d')."' And
			dtab.ia_assitype_to >= '".date('Y-m-d')."'
		Order By
			label		
		";
		
	} else {
		
		// else
			
		$query = "
		Select
			dtab.ia_assitype_auto As value,
			dtab.ia_assitype_name As label
		From
			ia_assignment_type dtab
		Where
			dtab.ia_assitype_customer = ".$customer_id." And
			dtab.ia_assicate_auto = ".$record_category." And
			dtab.ia_assitype_from <= '".$create_date."' And
			dtab.ia_assitype_to >= '".$create_date."'			
		Order By
			label
		";
			
	}
	
} else {
	
	// Script to display super user list
	$query = "
	Select
		dtab.ia_assitype_auto As value,
		Case
			When dtab.ia_assitype_active = 1
			Then Concat(cont.name, ' - ', cate.ia_assicate_name, ' - ', dtab.ia_assitype_name, ' (Active) ', Date(dtab.ia_assitype_from), '/',
				Date(dtab.ia_assitype_to))
			Else Concat(cont.name, ' - ', cate.ia_assicate_name, ' - ', dtab.ia_assitype_name, ' (InActve) ', Date(dtab.ia_assitype_from), '/',
				Date(dtab.ia_assitype_to))
		End As label
	From
		ia_assignment_type dtab Inner Join
		jm4_contact_details cont On cont.id = dtab.ia_assitype_customer Inner Join
		ia_assignment_category cate On cate.ia_assicate_auto = dtab.ia_assicate_auto
	Where
		dtab.ia_assicate_auto = ".$record_category."
	Order By
		label
	";
	
}

// Build the query

// Report the result query
// echo $query;

// Query the database
$db = JFactory::getDbo();
$db->setQuery($query);
$results = $db->loadObjectList();
$results_count = count($results);

// Results error reporting
// print_r($results);
// echo $results_count;

// Result header
$myXMLData = "";
$myXMLData .= "<?xml version='1.0' encoding='UTF-8'?>\n";
$myXMLData .= "<assignment_type>\n";
$myXMLData .= "<id>0</id>\n";
$myXMLData .= "<label>Please select</label>\n";

// Exit if no records found
if ($results_count > 0) {
	
	foreach ($results as $result){
		$myXMLData .= "<id>".$result->value."</id>\n";
		$myXMLData .= "<label>".$result->label."</label>\n";
		// $myXMLData .= "<description>".$result->description."</description>\n";

	}
}

// Result footer
$myXMLData .= "</assignment_type>\n";

// Report the data found
$myXMLData = str_replace("&", "and", $myXMLData);
echo $myXMLData;

?>
