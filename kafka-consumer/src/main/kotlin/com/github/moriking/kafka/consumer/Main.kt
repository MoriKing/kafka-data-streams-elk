package com.github.moriking.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

fun main() {
    val logger = LoggerFactory.getLogger(KafkaConsumer::class.java.name)
    //todo: create visualizing client
    val consumer: KafkaConsumer<String, Long> = createConsumer("alarms_count")

    while (true) {
        val records: ConsumerRecords<String, Long>? = consumer.poll(Duration.ofMillis(10000))

        //todo: update the client with records every 10 seconds
        if (records?.count()!! < 1) {
            logger.info("--------------->No records received!")
        } else {
            for (record in records!!) {
                println(record)
            }
        }
    }
}

fun createConsumer(topic: String): KafkaConsumer<String, Long> {
    val bootstrapServers = "localhost:9092"
    val groupId = "consumer-histogram"

    // consumer configs
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

    val consumer = KafkaConsumer<String, Long>(properties)
    consumer.subscribe(listOf(topic))
    return consumer
}
