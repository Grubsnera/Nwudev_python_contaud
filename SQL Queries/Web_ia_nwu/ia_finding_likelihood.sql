-- phpMyAdmin SQL Dump
-- version 4.9.1
-- https://www.phpmyadmin.net/
--
-- Host: sql23.jnb1.host-h.net
-- Generation Time: Jul 31, 2020 at 04:30 PM
-- Server version: 10.1.45-MariaDB-1~stretch
-- PHP Version: 5.6.40-0+deb8u12

USE `Ia_nwu`;

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

--
-- Drop table just while in development
--
DROP TABLE `ia_finding_likelihood`;
-- --------------------------------------------------------

--
-- Table structure for table `ia_finding_likelihood`
--

CREATE TABLE `ia_finding_likelihood` (
  `ia_findlike_auto` int(11) NOT NULL,
  `ia_findlike_value` text NOT NULL,
  `ia_findlike_name` varchar(50) NOT NULL,
  `ia_findlike_desc` text NOT NULL,
  `ia_findlike_active` text NOT NULL,
  `ia_findlike_from` datetime NOT NULL,
  `ia_findlike_to` datetime NOT NULL,
  `ia_findlike_createdate` datetime DEFAULT NULL,
  `ia_findlike_createby` varchar(50) DEFAULT NULL,
  `ia_findlike_editdate` datetime DEFAULT NULL,
  `ia_findlike_editby` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Table to store finding likelihoods';

--
-- Dumping data for table `ia_finding_likelihood`
--

INSERT INTO `ia_finding_likelihood` (`ia_findlike_auto`, `ia_findlike_value`, `ia_findlike_name`, `ia_findlike_desc`, `ia_findlike_active`, `ia_findlike_from`, `ia_findlike_to`, `ia_findlike_createdate`, `ia_findlike_createby`, `ia_findlike_editdate`, `ia_findlike_editby`) VALUES
(1, '0', 'No rating', 'No rating.', '1', '2020-01-01 00:00:00', '2099-12-31 00:00:00', '2020-07-31 16:43:00', 'Python', NULL, NULL),
(2, '1', 'Positive', '', '1', '2020-01-01 00:00:00', '2099-12-31 00:00:00', '2020-07-31 16:43:00', 'Python', '2020-07-31 15:40:27', 'Albertjvr'),
(3, '2', 'Housekeeping', '', '1', '2020-01-01 00:00:00', '2099-12-31 00:00:00', '2020-07-31 16:43:00', 'Python', '2020-07-31 15:41:35', 'Albertjvr'),
(4, '3', 'Minor', '', '1', '2020-01-01 00:00:00', '2099-12-31 00:00:00', '2020-07-31 16:43:00', 'Python', '2020-07-31 15:42:28', 'Albertjvr'),
(5, '4', 'Significant', '', '1', '2020-01-01 00:00:00', '2099-12-31 00:00:00', '2020-07-31 16:43:00', 'Python', '2020-07-31 15:43:32', 'Albertjvr'),
(6, '5', 'Critical', '', '1', '2020-01-01 00:00:00', '2099-12-31 00:00:00', '2020-07-31 16:43:00', 'Python', '2020-07-31 15:45:07', 'Albertjvr');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `ia_finding_likelihood`
--
ALTER TABLE `ia_finding_likelihood`
  ADD PRIMARY KEY (`ia_findlike_auto`),
  ADD KEY `fb_order_ia_findlike_name_INDEX` (`ia_findlike_name`),
  ADD KEY `fb_order_ia_findlike_from_INDEX` (`ia_findlike_from`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `ia_finding_likelihood`
--
ALTER TABLE `ia_finding_likelihood`
  MODIFY `ia_findlike_auto` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=7;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
