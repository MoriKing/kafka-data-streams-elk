package com.github.moriney.kafka.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*


internal class StreamsTest {
    private val stream = Streams()
    private val config: Properties = Properties()
    private lateinit var ttd: TopologyTestDriver
    private lateinit var inputTopic: TestInputTopic<String, String>

    init {
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "stream.topology.test")
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "foo:1234")
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
    }

    companion object {
        val TEST_RECORDS = listOf(
            "{\"affectedNode\":\"LX000228\",\"affectedSite\":\"LX000228\",\"alarmCategory\":\"FAULT\",\"alarmGroup\":" +
                    "\"003--936265541-SubNetwork=Osijek,MeContext=LX000228,ManagedElement=LX000228,ENodeBFunction=1,NbIotCell=gracani-1469856\"," +
                    "\"alarmCSN\":\"1469856\",\"alarmID\":\"9175114\",\"alarmMO\":\"SubNetwork=Osijek,MeContext=LX000228,ManagedElement=LX000228," +
                    "ENodeBFunction=1,NbIotCell=gracani\",\"alarmNotificationType\":\"Major\",\"alarmLastSeqNo\":\"1469856\",\"alarmEventTime\":" +
                    "\"2020-01-21T22:47:28+02:00\",\"vnocAlarmID\":\"ERA015\"}",

            "{\"affectedNode\":\"LX000220\",\"affectedSite\":\"LX000228\",\"alarmCategory\":\"FAULT\",\"alarmGroup\":" +
                    "\"003--936265541-SubNetwork=Osijek,MeContext=LX000228,ManagedElement=LX000228,ENodeBFunction=1,NbIotCell=gracani-1469856\"," +
                    "\"alarmCSN\":\"1469856\",\"alarmID\":\"9175114\",\"alarmMO\":\"SubNetwork=Osijek,MeContext=LX000228,ManagedElement=LX000228," +
                    "ENodeBFunction=1,NbIotCell=gracani\",\"alarmNotificationType\":\"Major\",\"alarmLastSeqNo\":\"1469856\",\"alarmEventTime\":" +
                    "\"2020-01-21T22:50:28+02:00\",\"vnocAlarmID\":\"ERA010\"}",

            "{\"affectedNode\":\"LX000220\",\"affectedSite\":\"LX000228\",\"alarmCategory\":\"FAULT\",\"alarmGroup\":" +
                    "\"003--936265541-SubNetwork=Osijek,MeContext=LX000228,ManagedElement=LX000228,ENodeBFunction=1,NbIotCell=gracani-1469856\"," +
                    "\"alarmCSN\":\"1469856\",\"alarmID\":\"9175114\",\"alarmMO\":\"SubNetwork=Osijek,MeContext=LX000228,ManagedElement=LX000228," +
                    "ENodeBFunction=1,NbIotCell=gracani\",\"alarmNotificationType\":\"Major\",\"alarmLastSeqNo\":\"1469856\",\"alarmEventTime\":" +
                    "\"2020-01-21T22:57:28+02:00\",\"vnocAlarmID\":\"ERA015\"}"
        )
    }

    @BeforeEach
    internal fun setUp() {
        ttd = TopologyTestDriver(stream.createTopology(), config)
        inputTopic = ttd.createInputTopic(Streams.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
    }

    @Test
    fun alarmsCountTest() {

        val outputTopicAlarmsCount =
            ttd.createOutputTopic(Streams.OUTPUT_TOPIC_ALARMS_COUNT, Serdes.String().deserializer(), Serdes.Long().deserializer());

        assert(outputTopicAlarmsCount.isEmpty)
        inputTopic.pipeValueList(TEST_RECORDS)

        val records = outputTopicAlarmsCount.readRecordsToList()
        assertEquals("ERA015", records.last().key)
        assertEquals(2, records.last().value)
        assertEquals("ERA010", records[records.lastIndex - 1].key)
        assertEquals(1, records[records.lastIndex - 1].value)
    }

    @Test
    fun nodesAlarmsCountTest() {
        val outputTopicNodesAlarmsCount =
            ttd.createOutputTopic(Streams.OUTPUT_TOPIC_NODES_ALARMS_COUNT, Serdes.String().deserializer(), Serdes.Long().deserializer());
        assert(outputTopicNodesAlarmsCount.isEmpty)
        inputTopic.pipeValueList(TEST_RECORDS)

        val records = outputTopicNodesAlarmsCount.readRecordsToList()
        assertEquals("LX000220", records.last().key)
        assertEquals(2, records.last().value)
        assertEquals("LX000228", records.first().key)
        assertEquals(1, records.first().value)

    }

    @Test
    fun hourEra015CountTest() {
        val outputTopicHourEra015Count =
            ttd.createOutputTopic(Streams.OUTPUT_TOPIC_HOUR_ERA015_COUNT, Serdes.String().deserializer(), Serdes.Long().deserializer());
        assert(outputTopicHourEra015Count.isEmpty)
        inputTopic.pipeValueList(TEST_RECORDS)

        val records = outputTopicHourEra015Count.readRecordsToList()
        assertEquals("2020-01-21T22", records.last().key)
        assertEquals(2, records.last().value)
    }

    @AfterEach
    fun tearDown() {
        ttd.close()
    }

    @Test
    fun convertToUtcHour() {
        assertEquals("2011-12-03 09:00:00",stream.convertToUtcHour("2011-12-03T10:15:30+01:00"))
        assertEquals("2011-12-03 06:00:00",stream.convertToUtcHour("2011-12-03T10:15:30+04:00"))
    }
}