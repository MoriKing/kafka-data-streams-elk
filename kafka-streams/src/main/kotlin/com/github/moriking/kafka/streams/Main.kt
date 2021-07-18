package com.github.moriking.kafka.streams

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import java.util.*

fun createTopology(): Topology {
    val builder = StreamsBuilder()
    val metadataRecords = builder.stream<String, String>("metadata")
    val alarmsCount = metadataRecords
        .mapValues { metaDatarecord -> parseMetaData(metaDatarecord)?.vnocAlarmID }
        .selectKey { _, value -> value }
        .groupByKey() //count occurrences
        .count(Materialized.`as`("Counts"))

    // write the results back to kafka
    alarmsCount.toStream().to("alarms-count", Produced.with(Serdes.String(), Serdes.Long()))
    return builder.build()
}

fun main() {
    val config = Properties()
    config[StreamsConfig.APPLICATION_ID_CONFIG] = "alarm-count-applications"
    config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
    config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    config[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    config[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    val streams = KafkaStreams(createTopology(), config)
    streams.start()

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(Thread { streams.close() })
}

fun parseMetaData(metaDataRecord: String): MetaData? {
    return try {
        val mapper = jacksonObjectMapper()
        mapper.readValue(metaDataRecord, MetaData::class.java)
    } catch (ex: Exception) {
        ex.printStackTrace()
        null
    }
}

data class MetaData(val affectedNode: String, val vnocAlarmID: String, val alarmEventTime: String)