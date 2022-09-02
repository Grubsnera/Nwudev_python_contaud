-- phpMyAdmin SQL Dump
-- version 4.9.1
-- https://www.phpmyadmin.net/
--
-- Host: sql23.jnb1.host-h.net
-- Generation Time: Jul 31, 2020 at 04:30 PM
-- Server version: 10.1.45-MariaDB-1~stretch
-- PHP Version: 5.6.40-0+deb8u12

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `Ia_nwu`
--

-- --------------------------------------------------------

--
-- Table structure for table `ia_finding_rate`
--

CREATE TABLE `ia_finding_rate` (
  `ia_findrate_auto` int(11) NOT NULL,
  `ia_findrate_impact` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `ia_findrate_name` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `ia_findrate_desc` text COLLATE utf8mb4_unicode_ci,
  `ia_findrate_active` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `ia_findrate_from` datetime NOT NULL,
  `ia_findrate_to` datetime NOT NULL,
  `ia_findrate_createdate` datetime DEFAULT NULL,
  `ia_findrate_createby` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ia_findrate_editdate` datetime DEFAULT NULL,
  `ia_findrate_editby` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Table to store finding status';

--
-- Dumping data for table `ia_finding_rate`
--

INSERT INTO `ia_finding_rate` (`ia_findrate_auto`, `ia_findrate_impact`, `ia_findrate_name`, `ia_findrate_desc`, `ia_findrate_active`, `ia_findrate_from`, `ia_findrate_to`, `ia_findrate_createdate`, `ia_findrate_createby`, `ia_findrate_editdate`, `ia_findrate_editby`) VALUES
(1, '0', 'No rating', 'No rating.', '1', '2018-01-01 00:00:00', '2099-12-31 00:00:00', '2018-12-07 06:57:11', 'Python', NULL, NULL),
(2, '1', 'Positive', 'No findings based on the results of audit performed. An event that does not hold any significant and/or immediate threat to the company or project.', '1', '2018-01-01 00:00:00', '2099-12-31 00:00:00', '2018-12-07 06:57:00', 'Python', '2020-07-31 15:40:27', 'Albertjvr'),
(3, '2', 'Housekeeping', 'Negligible impact on the business. An event that can be managed under normal operating conditions and which will have a minor impact on the company or project.', '1', '2018-01-01 00:00:00', '2099-12-31 00:00:00', '2018-12-07 06:57:00', 'Python', '2020-07-31 15:41:35', 'Albertjvr'),
(4, '3', 'Minor', 'Minor impact on the business. An event that can be managed should it occur, but will require additional resources and management effort.', '1', '2018-01-01 00:00:00', '2099-12-31 00:00:00', '2018-12-07 06:57:00', 'Python', '2020-07-31 15:42:28', 'Albertjvr'),
(5, '4', 'Significant', 'Significant impact on the business. Potentially significant risk exposure that can be endured, but may have a prolonged negative impact and extensive consequences.', '1', '2018-01-01 00:00:00', '2099-12-31 00:00:00', '2018-12-07 06:57:00', 'Python', '2020-07-31 15:43:32', 'Albertjvr'),
(6, '5', 'Critical', 'Critical impact on the business. Report to audit committee. A risk that is potentially disastrous to the company and that could fundamentally hinder the achievement of its objectives and ultimately lead to the collapse of the business or project.', '1', '2018-01-01 00:00:00', '2099-12-31 00:00:00', '2018-12-07 06:57:00', 'Python', '2020-07-31 15:45:07', 'Albertjvr');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `ia_finding_rate`
--
ALTER TABLE `ia_finding_rate`
  ADD PRIMARY KEY (`ia_findrate_auto`),
  ADD KEY `fb_order_ia_findrate_name_INDEX` (`ia_findrate_name`),
  ADD KEY `fb_order_ia_findrate_from_INDEX` (`ia_findrate_from`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `ia_finding_rate`
--
ALTER TABLE `ia_finding_rate`
  MODIFY `ia_findrate_auto` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=7;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
