# kafka-data-streams
Reads streams of data through Kafka and generates graphs:
* Histogram of alarm types and counts
* Histogram of nodes and alarm counts
* Timeline of ERA015 alarm counts per hour

# Architecture and requirements
The project is implemented in Kotlin and is build using Gradle 7.1.1. Docker and Docker Compose are required to run the project.

The project consits of:
- A stand-alone Kafka producer  
    * reads input data files from ./data
    * produces records into Kafka's "metadata" topic
- A stand-alone KafkaStreams stream  
    * processes incoming records and populates the results into three Kafka streams (alarms-count, nodes-alarms-count, hour-ERA015-count)
- A stand-alone Kafka consumer  
    * consumes output data from the stream and writes into a Jetty web server's [data viewer](https://github.com/jasrodis/dataviewer)
- Apache Kafka 
- Apache Zookeeper 

# How to run
To run the project demo on localhost execute the `run.sh`

`run.sh` 
- creates images for kafka stand-alone servers using Jib Gradle plugin
- runs created images locally along with Apache Kafka and Zookeeper clusters using Docker Compose with `kafka.yaml` setting file  
    * the host addresses and ports are configurable through `kafka.yaml`

# Input directory
- /data directory is mounted as the default input path for the producer and it includes the sample packages.json file

# Output data view
- Jetty server creates the data views on the following addresses:  
    * [localhost:8090/view/AlarmCount](localhost:8090/view/AlarmCount)
    * [localhost:8090/view/NodesAlarmCount](localhost:8090/view/NodesAlarmCount)
    * [localhost:8090/view/HourERA015Count](localhost:8090/view/HourERA015Count)

- The data views are available only after consumer receives its first record  
- data views should be refreshed manually to present the latest figures
