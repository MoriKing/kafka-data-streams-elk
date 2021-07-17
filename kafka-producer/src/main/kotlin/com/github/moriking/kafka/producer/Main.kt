package com.github.moriking.kafka.producer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import java.util.*

data class MetaData(val metadata: String)

fun readJson(): List<MetaData> {
    return try {
        val mapper = jacksonObjectMapper()
        val recordsList = listOf(
            *mapper.readValue(Paths.get("/home/ezneimo/packages.json").toFile(), Array<MetaData>::class.java)
        )
        recordsList
    } catch (ex: Exception) {
        ex.printStackTrace()
        emptyList()
    }
}

fun createKafkaProducer(): KafkaProducer<String, String> {
    val bootstrapServers = "0.0.0.0:9092"

    // create Producer properties
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer::class.java.name
    )
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer::class.java.name
    )

    // create safe Producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Int.MAX_VALUE))
    properties.setProperty(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        "5"
    ) // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.

    // high throughput producer (at the expense of a bit of latency and CPU usage)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32 * 1024).toString()) // 32 KB batch size

    // create the producer
    return KafkaProducer(properties)
}

fun main() {
    val logger = LoggerFactory.getLogger(MetaData::class.java.name)
    logger.info("Setting up the json producer application")

    val producer: KafkaProducer<String, String> = createKafkaProducer()

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("stopping the json application...")
        logger.info("closing producer...")
        producer.close();
        logger.info("done!")
    })
    val dataList = readJson()
    dataList.forEach {
        producer.send(ProducerRecord("metadata", null, it.metadata)) { _, e ->
            if (e != null) {
                logger.error("Something went wrong!", e)
            }
        }
    }

    logger.info("End of application")
}