package dev.ocpplab

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.awaitility.Awaitility.await
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.Duration

/**
 * Same context for every Kafka test class (Spring caches it), with the scheduled relay switched off:
 * tests call OutboxRelay.publishPending() themselves, so nothing is published behind their back.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = ["outbox.relay.enabled=false"])
@Import(TestcontainersConfiguration::class)
annotation class KafkaIntegrationTest

/** Reads the topic and watches consumer group progress, without Thread.sleep. */
class KafkaTestSupport(bootstrapServers: List<String>, private val topic: String) {

    private val servers = bootstrapServers.joinToString(",")

    /** Every record currently on the topic, from offset 0 up to the end offsets at the time of the call. */
    fun readAll(): List<ConsumerRecord<String, String>> {
        val props = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to servers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        )
        KafkaConsumer<String, String>(props).use { consumer ->
            val partitions = consumer.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
            consumer.assign(partitions)
            consumer.seekToBeginning(partitions)
            val end = consumer.endOffsets(partitions)
            val records = mutableListOf<ConsumerRecord<String, String>>()
            await().atMost(Duration.ofSeconds(15)).pollInSameThread().until {
                consumer.poll(Duration.ofMillis(200)).forEach { records += it }
                partitions.all { consumer.position(it) >= end.getValue(it) }
            }
            return records
        }
    }

    /** Waits until [groupId] has committed offsets up to the current end of every partition. */
    fun awaitConsumerGroupCaughtUp(groupId: String) {
        AdminClient.create(mapOf<String, Any>(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to servers)).use { admin ->
            val partitions = admin.describeTopics(listOf(topic)).allTopicNames().get().getValue(topic)
                .partitions().map { TopicPartition(topic, it.partition()) }
            val end = admin.listOffsets(partitions.associateWith { OffsetSpec.latest() }).all().get()
            await().atMost(Duration.ofSeconds(30)).pollInSameThread().until {
                val committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()
                partitions.all { p -> (committed[p]?.offset() ?: 0L) >= end.getValue(p).offset() }
            }
        }
    }
}
