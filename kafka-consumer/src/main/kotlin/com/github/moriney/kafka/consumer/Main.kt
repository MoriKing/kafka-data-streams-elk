package com.github.moriney.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.charts.dataviewer.api.trace.BarTrace
import org.charts.dataviewer.api.trace.TimeSeriesTrace
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

const val ALARMS_COUNT_TOPIC = "alarms-count"
const val NODES_ALARMS_COUNT_TOPIC = "nodes-alarms-count"
const val HOUR_ERA015_TOPIC = "hour-ERA015-count"

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(KafkaConsumer::class.java.name)
    if (args.isEmpty()) {
        logger.error("Server:port is not specified")
        return
    }
    val alarmCountPlot by lazy { Plot("Alarm Count", "alarms", "counts", ::BarTrace) }
    val nodesAlarmCountPlot by lazy { Plot("Nodes Alarm Count", "alarms", "counts", ::BarTrace, 150) }
    val hourEra015Plot by lazy { Plot("Hour ERA015 count", "", "counts", ::TimeSeriesTrace, 150) }
    val consumer: KafkaConsumer<String, Long> = createConsumer(args[0], listOf(ALARMS_COUNT_TOPIC, NODES_ALARMS_COUNT_TOPIC, HOUR_ERA015_TOPIC))

    // polling for new record
    while (true) {
        val records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE))
        for (record in records) {
            when (record.topic()) {
                ALARMS_COUNT_TOPIC -> alarmCountPlot.updateValue(record.key(), record.value())
                NODES_ALARMS_COUNT_TOPIC -> nodesAlarmCountPlot.updateValue(record.key(), record.value())
                HOUR_ERA015_TOPIC -> hourEra015Plot.updateValue(record.key(), record.value())
            }
            logger.info("Key: " + record.key() + ", Value: " + record.value())
            logger.info("Partition: " + record.partition() + ", Offset:" + record.offset())
        }
        if (!records.isEmpty) {
            alarmCountPlot.build()
            nodesAlarmCountPlot.build()
            hourEra015Plot.build()
        }
    }
}

fun createConsumer(bootstrapServers: String, topics: List<String>): KafkaConsumer<String, Long> {
    val groupId = "consumer-histogram"

    // consumer configs
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = KafkaConsumer<String, Long>(properties)
    consumer.subscribe(topics)
    return consumer
}
