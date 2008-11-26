<?php
$memcache;
$memcache = new Memcache;
$memcache->connect('localhost', 22201) or die ("Could not connect");

for ($i=0; $i < 100000; $i++) {
	$msg = "aaaaaaaaaaaaaaa";
  while(!$memcache->set('queue_001', $msg, false, 0)){
		$memcache->close();
    $memcache = new Memcache;
		$memcache->connect('localhost', 22201) or die ("Could not connect");
		// any logging here?
		echo "reconnect...\n";
	}
}

$memcache->close();
?>
