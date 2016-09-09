# JCConf2016 

A simple example zookeeper client for JCConf2016 

## Start Zookeeper Standalone Server

1. Download from [Apache ZooKeeper™ Releases](https://zookeeper.apache.org/releases.html)
2. Decompress to `{ZOOKEEPER_HOME}`
3. `{ZOOKEEPER_HOME}/bin/zkServer.sh start`

## Run Example Test

- `mvn -Dtest=ZooKeeperClientTest#testClient test`
- `mvn -Dtest=ZooKeeperClientTest#testSpringTask test`

### Optional Parameters

- `-DconnectString=localhost:2181`
- `-DrootPath=/jcconf2016` 
- `-DnumberOfParticipants=5`

