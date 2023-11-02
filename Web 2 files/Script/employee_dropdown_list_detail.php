<?php

// Script to obtain employee details based on the employee number
// Returning the employee name and email address
// Search on employee number
// 29 May 2022 Albert Janse van Rensburg


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
$search_for = "Searched for";
$search_for = $app->input->getString('param1');

// Report the parameters
//echo $search_for;

// Build the query
$query = "
Select
    peop.employee_number,
    peop.name_address,
    peop.email_address
From
    ia_people peop
Where
    peop.employee_number = '".$search_for."'
";

// Report the result query
//echo $query;

// Query the database
$db = JFactory::getDbo();
$db->setQuery($query);
$results = $db->loadObjectList();
$results_count = count($results);

// Results error reporting
//echo print_r($results);
//echo $results_count;

// Result header
$myXMLData = "";
$myXMLData .= "<?xml version='1.0' encoding='UTF-8'?>\n";
$myXMLData .= "<people>\n";

// Exit if no records found
if ($results_count > 0) {
	foreach ($results as $result){
		//echo print_r($result);
		$myXMLData .= "<peopid>".$result->employee_number."</peopid>\n";
		$myXMLData .= "<peopname>".$result->name_address."</peopname>\n";
		$myXMLData .= "<peopemail>".$result->email_address."</peopemail>\n";
	}
}

// Result footer
$myXMLData .= "</people>\n";

// Report the data found
$myXMLData = str_replace("&", "and", $myXMLData);
echo $myXMLData;

?>
