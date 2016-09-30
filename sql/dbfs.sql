DROP DATABASE IF EXISTS `wd2.data`;
CREATE DATABASE `wd2.data`;

USE `wd2.data`;

CREATE TABLE `dirs` (
    `username` VARCHAR(128) NOT NULL,
    `filename` VARCHAR(4096) NOT NULL,
    `parent` VARCHAR(512) NOT NULL,
    `bucket` VARCHAR(64) NOT NULL,
    `rkey` VARCHAR(256) NOT NULL,
    `mode` INT UNSIGNED NOT NULL,
    `size` BIGINT NOT NULL,
    `created` DATETIME NULL DEFAULT NULL,
    `modified` DATETIME NULL DEFAULT NULL,
    INDEX name (`username`, `filename`, `parent`, `bucket`),
    INDEX (`rkey`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;
