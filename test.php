<?php
include_once "vendor/autoload.php";

ini_set('display_errors', 'On');
error_reporting(-1);
set_time_limit(0);
echo "Start\n";
$parser = new \MysqlSlowLogParser\Parser('mysql:host=127.0.0.1;port=3306;charset=utf8', 'root', '');
$parser->go( __DIR__ . '/mysql-slow.log');
echo "Done\n";