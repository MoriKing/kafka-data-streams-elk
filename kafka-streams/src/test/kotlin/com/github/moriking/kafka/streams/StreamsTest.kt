package com.github.moriking.kafka.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.*


internal class StreamsTest {
    private val stream = Streams()
    private val config: Properties = Properties()
    lateinit var ttd: TopologyTestDriver


    init {
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "stream.topology.test")
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "foo:1234")
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
    }

    @Test
    fun topologyTest() {
        val topology = stream.createTopology()
        ttd = TopologyTestDriver(topology, config)
        val inputTopic = ttd.createInputTopic(stream.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        val outputTopicAlarmsCount = ttd.createOutputTopic(stream.OUTPUT_TOPIC_ALARMS_COUNT, Serdes.String().deserializer(), Serdes.Long().deserializer());
        val outputTopicNodesAlarmsCount = ttd.createOutputTopic(stream.OUTPUT_TOPIC_NODES_ALARMS_COUNT, Serdes.String().deserializer(), Serdes.Long().deserializer());
        val outputTopicHourEra015Count = ttd.createOutputTopic(stream.OUTPUT_TOPIC_HOUR_ERA015_COUNT, Serdes.String().deserializer(), Serdes.Long().deserializer());

        //assert(inputTopic.)
        assert(outputTopicAlarmsCount.isEmpty)
        assert(outputTopicNodesAlarmsCount.isEmpty)
        assert(outputTopicHourEra015Count.isEmpty)

        inputTopic.pipeInput(null,"{\"affectedNode\":\"LX000228\",\"affectedSite\":\"LX000228\",\"alarmCategory\":\"FAULT\",\"alarmGroup\":" +
                "\"003--936265541-SubNetwork=Osijek,MeContext=LX000228,ManagedElement=LX000228,ENodeBFunction=1,NbIotCell=gracani-1469856\"," +
                "\"alarmCSN\":\"1469856\",\"alarmID\":\"9175114\",\"alarmMO\":\"SubNetwork=Osijek,MeContext=LX000228,ManagedElement=LX000228," +
                "ENodeBFunction=1,NbIotCell=gracani\",\"alarmNotificationType\":\"Major\",\"alarmLastSeqNo\":\"1469856\",\"alarmEventTime\":" +
                "\"2020-01-21T22:47:28+02:00\",\"vnocAlarmID\":\"ERA015\"}")

//        inputTopic.pipeValueList(
//            listOf("{\"affectedNode\":\"LX000228\",\"affectedSite\":\"LX000228\",\"alarmCategory\":\"FAULT\",\"alarmGroup\":" +
//                "\"003--936265541-SubNetwork=Osijek,MeContext=LX000228,ManagedElement=LX000228,ENodeBFunction=1,NbIotCell=gracani-1469856\"," +
//                "\"alarmCSN\":\"1469856\",\"alarmID\":\"9175114\",\"alarmMO\":\"SubNetwork=Osijek,MeContext=LX000228,ManagedElement=LX000228," +
//                "ENodeBFunction=1,NbIotCell=gracani\",\"alarmNotificationType\":\"Major\",\"alarmLastSeqNo\":\"1469856\",\"alarmEventTime\":" +
//                "\"2020-01-21T22:47:28+02:00\",\"vnocAlarmID\":\"ERA015\"}"))


        var record = outputTopicAlarmsCount.readRecord()
        assertEquals("ERA015", record.key)
        //assertEquals("1", record.value)

        record = outputTopicNodesAlarmsCount.readRecord()
        assertEquals("LX000228", record.key)
        assertEquals(1, record.value)

        record = outputTopicHourEra015Count.readRecord()
        assertEquals("2020-01-21T22", record.key)
        assertEquals(1, record.value)
    }

    @AfterEach
    fun tearDown() {
        ttd.close()
    }
}