<?php

// Script to obtain remediation response details based on the response number
// Returning the response name
// Search on response number
// 1 June 2022 Albert Janse van Rensburg

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
$search_for = "";
$search_for = JFactory::getApplication()->input->getString('param1');

// Report the parameters
//echo $search_for;

// Build the query
$query = "
Select
    resp.ia_findresp_desc
From
    ia_finding_response resp
Where
    resp.ia_findresp_auto = '".$search_for."'
";

// Report the result query
//echo $query;

// Query the database
$db = JFactory::getDbo();
$db->setQuery($query);
$results = $db->loadObjectList();
$results_count = count($results);

// Results error reporting
//print_r($results);
//echo $results_count;

// Result header
$myXMLData = "";
$myXMLData .= "<?xml version='1.0' encoding='UTF-8'?>\n";
$myXMLData .= "<response>\n";

// Exit if no records found
if ($results_count > 0) {
	foreach ($results as $result){
		$myXMLData .= "<description>".$result->ia_findresp_desc."</description>\n";
	}
}

// Result footer
$myXMLData .= "</response>\n";

// Report the data found
$myXMLData = str_replace("&", "and", $myXMLData);
echo $myXMLData;

?>
