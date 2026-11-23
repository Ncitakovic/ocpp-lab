package dev.ocpplab.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import dev.ocpplab.KafkaIntegrationTest
import dev.ocpplab.KafkaTestSupport
import dev.ocpplab.billing.BillingListener
import dev.ocpplab.billing.BillingRecordRepository
import dev.ocpplab.billing.ProcessedEventRepository
import dev.ocpplab.events.EventEnvelope
import dev.ocpplab.events.SESSION_CLOSED
import dev.ocpplab.events.SESSION_OPENED
import dev.ocpplab.session.SessionService
import dev.ocpplab.session.StopOutcome
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails
import org.springframework.kafka.core.KafkaTemplate
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Session change -> outbox row -> relay -> Kafka, against real Postgres and Kafka (Testcontainers).
 * The relay is called directly (scheduler is off), so every test knows exactly what was published.
 */
@KafkaIntegrationTest
class OutboxKafkaIntegrationTest @Autowired constructor(
    private val sessions: SessionService,
    private val relay: OutboxRelay,
    private val outboxStore: JdbcOutboxStore,
    private val billingRecords: BillingRecordRepository,
    private val processedEvents: ProcessedEventRepository,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val mapper: ObjectMapper,
    private val clock: Clock,
    connection: KafkaConnectionDetails,
    @param:Value("\${outbox.topic}") private val topic: String,
) {
    private val kafka = KafkaTestSupport(connection.bootstrapServers, topic)
    private val startedAt = Instant.parse("2026-11-20T09:00:00Z")
    private val stoppedAt = Instant.parse("2026-11-20T10:00:00Z")

    /** Publish whatever earlier tests left behind, so each test starts with an empty outbox. */
    @BeforeEach
    fun drainOutbox() {
        while (relay.publishPending() > 0) { /* keep going */ }
    }

    private fun openSession(): Int =
        sessions.startSession("CP001", 1, "TAG-1", meterStart = 1000, startedAt = startedAt).transactionId!!

    private fun outboxRows(id: Int, type: String) = outboxStore.findByAggregateId(id.toString()).filter { it.type == type }

    private fun envelope(record: ConsumerRecord<String, String>) = mapper.readValue(record.value(), EventEnvelope::class.java)

    /** SessionClosed records on the topic for this transaction. */
    private fun closedOnTopic(id: Int) =
        kafka.readAll().filter { it.key() == id.toString() && envelope(it).type == SESSION_CLOSED }

    @Test
    fun `starting a session writes SessionOpened to the outbox`() {
        val id = openSession()

        val row = outboxRows(id, SESSION_OPENED).single()
        assertNull(row.publishedAt)
        assertEquals(1000, mapper.readTree(row.payload)["meterStart"].asInt())
    }

    @Test
    fun `closing a session publishes exactly one event, keyed by transactionId`() {
        val id = openSession()

        assertEquals(StopOutcome.Closed, sessions.stopSession(id, 5500, stoppedAt, "EVDisconnected"))
        relay.publishPending()

        val record = closedOnTopic(id).single()
        assertEquals(id.toString(), record.key())
        val env = envelope(record)
        assertEquals(id.toString(), env.aggregateId)
        assertEquals(1000, env.payload["meterStart"].asInt())
        assertEquals(5500, env.payload["meterStop"].asInt())
        assertEquals("EVDisconnected", env.payload["reason"].asText())
        assertEquals(stoppedAt, Instant.parse(env.payload["stoppedAt"].asText()))

        val row = outboxRows(id, SESSION_CLOSED).single()
        assertEquals(row.id, env.eventId)
        assertNotNull(row.publishedAt, "published_at is set after the broker ack")
    }

    @Test
    fun `duplicate and unknown StopTransaction write no outbox row`() {
        val id = openSession()
        sessions.stopSession(id, 5500, stoppedAt, "Local")

        assertEquals(StopOutcome.AlreadyClosed, sessions.stopSession(id, 5500, stoppedAt, "Local"))
        assertEquals(StopOutcome.UnknownTransaction, sessions.stopSession(-1, 5500, stoppedAt, "Local"))

        assertEquals(1, outboxRows(id, SESSION_CLOSED).size)
        assertTrue(outboxStore.findByAggregateId("-1").isEmpty())
    }

    @Test
    fun `two identical concurrent StopTransactions give one outbox row and one event`() {
        val id = openSession()
        val threads = 2
        val ready = CountDownLatch(threads)
        val go = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(threads)

        try {
            val futures = (1..threads).map {
                pool.submit(
                    Callable {
                        ready.countDown()
                        go.await() // both threads are released at the same moment
                        sessions.stopSession(id, 5500, stoppedAt, "Local")
                    },
                )
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS), "workers did not start")
            go.countDown()
            val outcomes = futures.map { it.get(10, TimeUnit.SECONDS) }
            assertEquals(1, outcomes.count { it == StopOutcome.Closed }, "outcomes: $outcomes")
        } finally {
            pool.shutdownNow()
        }

        assertEquals(1, outboxRows(id, SESSION_CLOSED).size)
        relay.publishPending()
        assertEquals(1, closedOnTopic(id).size)
    }

    private class SimulatedCrash : RuntimeException("relay crashed after send, before published_at")

    @Test
    fun `relay crash after send but before published_at resends the event, billing stays at one`() {
        val id = openSession()
        relay.publishPending() // SessionOpened out of the way, so the next batch is only SessionClosed
        sessions.stopSession(id, 5500, stoppedAt, "Local")
        val eventId = outboxRows(id, SESSION_CLOSED).single().id

        // Same relay code, but the store "dies" on the UPDATE that would set published_at.
        val crashingStore = object : OutboxStore by outboxStore {
            override fun markPublished(id: UUID, publishedAt: Instant) = throw SimulatedCrash()
        }
        val crashingRelay = OutboxRelay(crashingStore, kafkaTemplate, mapper, clock, topic, 100, 10_000)

        assertFailsWith<SimulatedCrash> { crashingRelay.publishPending() }
        assertNull(outboxRows(id, SESSION_CLOSED).single().publishedAt, "still unpublished after the crash")

        // "Restart": the normal relay finds the row still unpublished and sends it again.
        assertEquals(1, relay.publishPending())
        assertNotNull(outboxRows(id, SESSION_CLOSED).single().publishedAt)

        val onTopic = closedOnTopic(id)
        assertEquals(2, onTopic.size, "sent twice: at-least-once")
        assertTrue(onTopic.all { envelope(it).eventId == eventId }, "both copies carry the same eventId")

        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            assertEquals(1, billingRecords.findByTransactionId(id).size)
        }
        kafka.awaitConsumerGroupCaughtUp(BillingListener.GROUP_ID) // the second copy has been handled too
        assertEquals(1, billingRecords.findByTransactionId(id).size)
        assertTrue(processedEvents.exists(eventId))
    }
}
