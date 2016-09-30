DROP DATABASE IF EXISTS `wd2.auth`;
CREATE DATABASE `wd2.auth`;

USE `wd2.auth`;

CREATE TABLE `users` (
    `username` VARCHAR(128) NOT NULL,
    `password` VARCHAR(64) NOT NULL,
    `created` DATETIME NULL DEFAULT NULL,
    PRIMARY KEY (`username`),
    UNIQUE (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

