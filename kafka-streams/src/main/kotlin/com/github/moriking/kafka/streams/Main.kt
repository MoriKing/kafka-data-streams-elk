package com.github.moriking.kafka.streams

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.slf4j.LoggerFactory
import java.util.*

val logger = LoggerFactory.getLogger(StreamsBuilder::class.java.name)

fun createTopology(): Topology {
    val builder = StreamsBuilder()
    val metadataRecords = builder.stream<String, String>("metadata")

    //alarms-count
    val alarmsCountTopology: KTable<String?, Long>? = metadataRecords
        .mapValues { metaDataRecord -> parseMetaData(metaDataRecord)?.vnocAlarmID }
        .selectKey { _, value -> value }
        .groupByKey()
        .count(Materialized.`as`("Counts"))

    //nodes-alarms-count
    val nodesAlarmsCountTopology: KTable<String?, Long>? = metadataRecords
        .selectKey { _, metaDataRecord -> parseMetaData(metaDataRecord)?.affectedNode }
        .mapValues { metaDataRecord -> parseMetaData(metaDataRecord)?.vnocAlarmID }
        .groupByKey()
        .count(Materialized.`as`("Counts"))

    //hour-ERA015-count : all timestamps are considered to belong to the same time zone for simplicity
    val hourEra015CountTopology: KTable<String?, Long>? = metadataRecords
        .selectKey { _, metaDataRecord -> parseMetaData(metaDataRecord)?.alarmEventTime?.subSequence(0, 13).toString() }
        .mapValues { metaDataRecord -> parseMetaData(metaDataRecord)?.vnocAlarmID }
        .filter { _, value -> value == "ERA015" }
        .groupByKey()
        .count(Materialized.`as`("Counts"))


    // write the results back to kafka topics
    alarmsCountTopology?.toStream()?.to("alarms-count", Produced.with(Serdes.String(), Serdes.Long()))
    nodesAlarmsCountTopology?.toStream()?.to("nodes-alarms-count", Produced.with(Serdes.String(), Serdes.Long()))
    nodesAlarmsCountTopology?.toStream()?.to("hour-ERA015-count", Produced.with(Serdes.String(), Serdes.Long()))

    return builder.build()
}

fun main() {
    val config = Properties()
    config[StreamsConfig.APPLICATION_ID_CONFIG] = "alarm-count-applications"
    config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
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
        logger.info("One metadata record is processed...")
        val mapper = jacksonObjectMapper()
        mapper.readValue(metaDataRecord, MetaData::class.java)
    } catch (ex: Exception) {
        ex.printStackTrace()
        null
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class MetaData(val affectedNode: String?, val vnocAlarmID: String?, val alarmEventTime: String?)