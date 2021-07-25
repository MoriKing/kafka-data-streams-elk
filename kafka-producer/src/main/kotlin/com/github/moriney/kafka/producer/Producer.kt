package com.github.moriney.kafka.producer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import java.io.File

class Producer(private val logger: Logger, private val kafkaProducer: KafkaProducer<String, String>) {
    fun close() {
        kafkaProducer.close()
    }

    fun processFile(file: File) {
        if (file.isFile)
            process(file)
    }

    private fun process(file: File) = readJson(file).forEach { data ->
        kafkaProducer.send(ProducerRecord("metadata", null, data.metadata)) { _, e ->
            if (e != null) {
                logger.error("Couldn't send metadata", e)
            }
        }
        logger.info("Metadata is sent to Kafka:" + data.metadata)
    }

    private fun readJson(file: File): List<MetaData> {
        return try {
            if (!file.isFile) {
                emptyList()
            } else {
                val mapper = jacksonObjectMapper()
                val recordsList = listOf(*mapper.readValue(file, Array<MetaData>::class.java))
                recordsList
            }
        } catch (ex: Exception) {
            logger.error("Couldn't read ${file.name}", ex)
            emptyList()
        }
    }
}

private data class MetaData(val metadata: String)