package com.github.moriking.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

fun main() {
    val logger = LoggerFactory.getLogger(KafkaConsumer::class.java.name)
    val plot = Plot("Alarm Count", "alarms", "counts")
    val consumer: KafkaConsumer<String, Long> = createConsumer("alarms-count")

    // polling for new record
    while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        for (record in records) {
            logger.info("Key: " + record.key() + ", Value: " + record.value())
            logger.info("Partition: " + record.partition() + ", Offset:" + record.offset())
            plot.updateValue(record.key(), record.value())
        }
        plot.build()
        //todo: update the client with records every 10 seconds
    }
}

fun createConsumer(topic: String): KafkaConsumer<String, Long> {
    val bootstrapServers = "localhost:9092"
    val groupId = "consumer-histogram"

    // consumer configs
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = KafkaConsumer<String, Long>(properties)
    consumer.subscribe(listOf(topic))
    return consumer
}
