CREATE TABLE `bars_1min` (
  `id` int NOT NULL AUTO_INCREMENT,
  `ticker` varchar(32) NOT NULL,
  `datetime` datetime NOT NULL,
  `open` decimal(19,4) NULL,
  `high` decimal(19,4) NULL,
  `low` decimal(19,4) NULL,
  `close` decimal(19,4) NULL,
  `volume` bigint NULL,
  `open_interest` int NULL,
  KEY (`id`),
  PRIMARY KEY (`ticker`, `datetime`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;