package com.github.moriking.kafka.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Paths
import java.nio.file.StandardWatchEventKinds
import java.util.*

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(Producer::class.java.name)
    if (args.isEmpty()) {
        logger.error("Directory is not specified")
        return
    }

    logger.info("Setting up the json producer application")
    val producer = Producer(logger, createKafkaProducer())
    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("stopping the json application...")
        logger.info("closing producer...")
        producer.close()
        logger.info("done!")
    })

    File(args[0]).walk().maxDepth(1).onFail { file, exception ->
        logger.error("Couldn't access file list of ${file.name}", exception)
    }.forEach {
        producer.processFile(it)
    }

    val watchService = FileSystems.getDefault().newWatchService()
    val path = Paths.get(args[0])
    path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE)
    while (true) {
        val watchKey = watchService.take()
        watchKey?.pollEvents()?.forEach {
            producer.processFile(File(args[0] + File.separator + it.context()))
        }
        watchKey?.reset()
    }
}

fun createKafkaProducer(): KafkaProducer<String, String> {
    val bootstrapServers = "localhost:9092"
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

    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Int.MAX_VALUE.toString())
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32 * 1024).toString()) // 32 KB batch size

    return KafkaProducer(properties)
}