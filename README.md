# JCConf2016 

A simple example zookeeper client for JCConf2016 

## Start Zookeeper server in standalone mode

1. Download from [Apache ZooKeeper™ Releases](https://zookeeper.apache.org/releases.html)
2. `/bin/zkServer.sh start`

## Run example test

- `mvn -Dtest=ZooKeeperClientTest#testClient test`
- `mvn -Dtest=ZooKeeperClientTest#testSpringTask test`

### Optional parameters

- `-DconnectString=localhost:2181`
- `-DrootPath=/jcconf2016` 
- `-DnumberOfParticipants=5`

### Requires

- [Apache Maven](https://maven.apache.org) installed
- JDK8
