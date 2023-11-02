<?php

// Script to obtain assignment status based on assignment category
// Returning the response name
// Search on response number
// 21 June 2023 Albert Janse van Rensburg

$dir = __DIR__;
echo 'DIR: '.$dir.'<br>';
$files = scandir($dir);
foreach ($files as $file) {
echo $file . '<br>';
}
echo '<br>';

// Setup joomla to run an external php
//define('_JEXEC', 1);    
//use Joomla\CMS\Factory;
define('JPATH_BASE', $dir.'/..');
echo 'JPATH_BASE: '.JPATH_BASE.'<br>';
//require_once JPATH_BASE . '/includes/defines.php';
//require_once JPATH_BASE . '/includes/framework.php';
//error_reporting(E_ALL);
//ini_set('display_errors', 1);
//$crlf = PHP_EOL;
//$app = new /Joomla/CMS/Application/SiteApplication();
//Joomla/CMS/Factory::$application = $app;

require_once JPATH_BASE . '/libraries/vendor/autoload.php';
//use Mpdf/Mpdf;
//$mpdf = new Mpdf();
$mpdf = new /Mpdf/Mpdf();
//$mpdf->showImageErrors = true;
//$mpdf->WriteHTML('<h1>Hello, mPDF!</h1>'); // Add content to the PDF
//$mpdf->Output(); // Output the PDF to the browser

?>
