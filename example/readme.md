# Function
```
The example is to show how to send data from hydra to IoTDB through Kafka.
```
# Usage
## Dependencies with Maven

```
<dependencies>
    <dependency>
    	<groupId>org.apache.kafka</groupId>
    	<artifactId>kafka_2.10</artifactId>
    	<version>0.8.2.0</version>
    </dependency>
    <dependency>
    	<groupId>cn.edu.tsinghua</groupId>
     	<artifactId>iotdb-jdbc</artifactId>
     	<version>0.1.2</version>
    </dependency>
</dependencies>
```

## Launch the servers

```
  Before you run the program, make sure you have launched the servers of Kafka and IoTDB.
  For details, please refer to http://kafka.apache.org/081/documentation.html#quickstart
  If you are using windows, you may refer to http://blog.csdn.net/evankaka/article/details/52421314 
  and http://blog.csdn.net/yuebao1991/article/details/72771599 
  when you come across some issues.
```

## Run hydra
### modify configuration
Please make sure you have `conf\config.properties` under hydra's root directory if you are using intelliJ. For eclipse and other IDE users, figure out where to put this file yourself.  
Then open `conf\config.properties`,   
set `qa81.kafkaUrl=127.0.0.1:9092` to your Kafka url,   
and set `KMXRecordType=json` as given.

### run RealTimeDataTool.java
You may find this java file in   
`hydra\hydra-k2platform-parent\hydra-k2platform-tools-parent\hydra-k2platform-qatools\src\main\java\com\k2data\e2e\tool\RealTimeDataTool.java`  
Run it with option "sdo", so that the generated data will be put onto Kafka.


## Run KafkaConsumer.java

```
  The program is to consume data from Kafka.
  You can set the parameter of threadsNum to make sure the number of consumer threads:(for example: "5")
  > private final static int threadsNum = 5;
```

