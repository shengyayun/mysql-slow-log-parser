<?php
namespace MysqlSlowLogParser;

class Parser{

	public function __construct($dsn, $user, $pwd, $scheme = 'test', $table = 'mysql_slow_logs'){
		$this->pdo = new \PDO($dsn, $user, $pwd);
		$this->scheme = $scheme;
		$this->table = $table;
		$this->pdo->query("create database if not exists {$this->scheme} charset utf8 COLLATE utf8_general_ci;");
		$this->pdo->query("drop table if exists {$this->scheme}.{$this->table};");
		$this->pdo->query("create table if not exists {$this->scheme}.{$this->table}(
				`id` int not null primary key auto_increment,
			    `Time` datetime not null,
			    `User` varchar(30) not null,
			    `Host` varchar(30) not null,
			    `Query_time` decimal(10,6) not null,
			    `Lock_time` decimal(10,6) not null,
			    `Rows_sent` int not null,
			    `Rows_examined` int not null,
			    `Content` varchar(255) not null
			)engine = innodb;"
		);
	}
	
	public function insert($item){
		$sql = "insert into {$this->scheme}.{$this->table}(`Time`,`User`,`Host`,`Query_time`,`Lock_time`,`Rows_sent`,`Rows_examined`,`Content`)
			values(:Time,:User,:Host,:Query_time,:Lock_time,:Rows_sent,:Rows_examined,:Content)";
		$sth = $this->pdo->prepare($sql, array(\PDO::ATTR_CURSOR => \PDO::CURSOR_FWDONLY));
		$ret = $sth->execute(array(
			':Time' => $item['Time'],
			':User' => $item['User'],
			':Host' => $item['Host'],
			':Query_time' => $item['Query_time'],
			':Lock_time' => $item['Lock_time'],
			':Rows_sent' => $item['Rows_sent'],
			':Rows_examined' => $item['Rows_examined'],
			':Content' => substr($item['Content'], 0, 255)
		));
		if (!$ret){
			var_dump($item);
			var_dump($sth->errorInfo());
		}
	}

	public function go($path){
		$file = fopen($path, "r");
		$item = false;
		while(! feof($file))
		{
			$row= trim(fgets($file));
			//# Time: 2017-09-28T15:46:34.468312Z
			if (preg_match("/^# Time: (.*)/", $row, $matches)){
				if ($item != false){
					$this->insert($item);
				}
				$item = array();
				$item['Time'] = date('Y-m-d H:i:s', strtotime($matches[1]) + 3600 * 8);
				continue;
			}
			//# User@Host: csl[csl] @  [172.16.161.124]  Id:  2131
			if (preg_match("/^# User@Host: .*?\[(.*?)\].*?\[(.*?)\]/", $row, $matches)){
				if ($item == false){
					continue;
				}
				$item['User'] = $matches[1];
				$item['Host'] = $matches[2];
				continue;
			}
			//# Query_time: 1.301761  Lock_time: 0.000350 Rows_sent: 1  Rows_examined: 1223920
			if (preg_match("/^# Query_time: ([\d.]+)  Lock_time: ([\d.]+) Rows_sent: ([\d.]+)  Rows_examined: ([\d.]+)/", $row, $matches)){
				if ($item == false){
					continue;
				}
				$item['Query_time'] = $matches[1];
				$item['Lock_time'] = $matches[2];
				$item['Rows_sent'] = $matches[3];
				$item['Rows_examined'] = $matches[4];
				continue;
			}
			//过滤注释、设置时间戳、use db
			if (preg_match("/^[^#]/", $row) && !preg_match("/^SET timestamp=/", $row) && !preg_match("/^use [^;]+;$/", $row)) {
				if ($item == false){
					continue;
				}
				$item['Content'] = array_key_exists('Content', $item) ? ($item['Content'] . ' ' . $row) : $row;
			}
		}
		fclose($file);




		
	}
}