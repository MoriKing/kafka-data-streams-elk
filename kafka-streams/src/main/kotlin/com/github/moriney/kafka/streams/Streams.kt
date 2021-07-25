package com.github.moriney.kafka.streams

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

class Streams(private val logger: Logger = LoggerFactory.getLogger(Streams::class.java.name)) {
    companion object {
        const val INPUT_TOPIC = "metadata"
        const val OUTPUT_TOPIC_ALARMS_COUNT = "alarms-count"
        const val OUTPUT_TOPIC_NODES_ALARMS_COUNT = "nodes-alarms-count"
        const val OUTPUT_TOPIC_HOUR_ERA015_COUNT = "hour-ERA015-count"
    }

    fun createTopology(): Topology {
        val builder = StreamsBuilder()

        val alarmsCountStream = builder.stream<String, String>(INPUT_TOPIC)
        val nodesAlarmsCountStream = builder.stream<String, String>(INPUT_TOPIC)
        val hourEra015CountStream = builder.stream<String, String>(INPUT_TOPIC)

        //alarms-count
        val alarmsCountTopology: KTable<String?, Long> = alarmsCountStream
            .mapValues { metaDataRecord -> parseMetaData(metaDataRecord)?.vnocAlarmID }
            .filter { _, value -> value != null }
            .selectKey { _, value -> value }
            .groupByKey()
            .count(Materialized.`as`("AlarmsCountsStore"))

        //nodes-alarms-count
        val nodesAlarmsCountTopology: KTable<String?, Long> = nodesAlarmsCountStream
            .selectKey { _, metaDataRecord -> parseMetaData(metaDataRecord)?.affectedNode }
            .mapValues { metaDataRecord -> parseMetaData(metaDataRecord)?.vnocAlarmID }
            .filter { key, value -> key != null && value != null }
            .groupByKey()
            .count(Materialized.`as`("NodesAlarmsCountsStore"))

        //hour-ERA015-count : all timestamps are considered to belong to the same time zone for simplicity
        val hourEra015CountTopology: KTable<String?, Long> = hourEra015CountStream
            .selectKey { _, metaDataRecord -> parseMetaData(metaDataRecord)?.alarmEventTime?.let { convertToUtcHour(it) } }
            .mapValues { metaDataRecord -> parseMetaData(metaDataRecord)?.vnocAlarmID }
            .filter { key, value -> key != null && value == "ERA015" }
            .groupByKey()
            .count(Materialized.`as`("HourEra015CountsStore"))


        // write the results back to kafka topics
        alarmsCountTopology.toStream()?.to(OUTPUT_TOPIC_ALARMS_COUNT, Produced.with(Serdes.String(), Serdes.Long()))
        nodesAlarmsCountTopology.toStream()?.to(OUTPUT_TOPIC_NODES_ALARMS_COUNT, Produced.with(Serdes.String(), Serdes.Long()))
        hourEra015CountTopology.toStream()?.to(OUTPUT_TOPIC_HOUR_ERA015_COUNT, Produced.with(Serdes.String(), Serdes.Long()))

        return builder.build()
    }

    private fun parseMetaData(metaDataRecord: String): MetaData? {
        return try {
            val mapper = jacksonObjectMapper()
            mapper.readValue(metaDataRecord, MetaData::class.java)
        } catch (ex: Exception) {
            logger.error("Unable to parse metadata record", ex)
            null
        }
    }

    fun convertToUtcHour(timeStamp: String): String? {
        return try {
            val isoOffsetTimeStamp = ZonedDateTime.parse(timeStamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .withZoneSameInstant(ZoneOffset.UTC)
                .toLocalDateTime()
            val hourlyMappedPattern = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:00:00")
            isoOffsetTimeStamp.format(hourlyMappedPattern)
        } catch (ex: Exception) {
            logger.error("Unable to parse timestamp", ex)
            null
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class MetaData(val affectedNode: String?, val vnocAlarmID: String?, val alarmEventTime: String?)
}