# ************************************************************
# Sequel Ace SQL dump
# Version 20096
#
# https://sequel-ace.com/
# https://github.com/Sequel-Ace/Sequel-Ace
#
# Host: tools-db (MySQL 5.5.5-10.6.22-MariaDB-log)
# Database: s55078__wdrc_p
# Generation Time: 2026-04-01 12:06:23 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
SET NAMES utf8mb4;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE='NO_AUTO_VALUE_ON_ZERO', SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table creations
# ------------------------------------------------------------

CREATE TABLE `creations` (
  `q` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `timestamp` varchar(14) NOT NULL,
  PRIMARY KEY (`q`),
  KEY `timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_general_ci;



# Dump of table deletions
# ------------------------------------------------------------

CREATE TABLE `deletions` (
  `q` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `timestamp` varchar(14) NOT NULL,
  PRIMARY KEY (`q`),
  KEY `timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_general_ci;



# Dump of table labels
# ------------------------------------------------------------

CREATE TABLE `labels` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `item` int(10) unsigned NOT NULL,
  `revision` int(10) unsigned NOT NULL,
  `timestamp` varchar(14) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '',
  `change_type` enum('added','changed','removed') CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT 'added',
  `language` int(11) unsigned NOT NULL,
  `type` enum('labels','descriptions','aliases','sitelinks') CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT 'labels',
  PRIMARY KEY (`id`),
  KEY `language` (`language`),
  KEY `timestamp` (`timestamp`,`change_type`,`language`,`type`),
  KEY `item` (`item`,`timestamp`),
  KEY `timestamp_2` (`timestamp`),
  CONSTRAINT `labels_ibfk_1` FOREIGN KEY (`language`) REFERENCES `texts` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;



# Dump of table meta
# ------------------------------------------------------------

CREATE TABLE `meta` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `key` varchar(32) NOT NULL DEFAULT '',
  `value` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `key` (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;



# Dump of table redirects
# ------------------------------------------------------------

CREATE TABLE `redirects` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `source` int(11) NOT NULL,
  `target` int(11) NOT NULL,
  `timestamp` varchar(14) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `source` (`source`,`target`),
  KEY `timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



# Dump of table statements
# ------------------------------------------------------------

CREATE TABLE `statements` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `item` int(11) unsigned NOT NULL,
  `revision` int(11) unsigned NOT NULL,
  `property` int(11) unsigned NOT NULL,
  `timestamp` varchar(14) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '',
  `change_type` enum('added','changed','removed') CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT 'added',
  PRIMARY KEY (`id`),
  UNIQUE KEY `property` (`property`,`timestamp`,`change_type`),
  KEY `item` (`item`,`timestamp`),
  KEY `timestamp` (`timestamp`),
  KEY `property_2` (`property`,`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;



# Dump of table texts
# ------------------------------------------------------------

CREATE TABLE `texts` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `value` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `value` (`value`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;




/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
