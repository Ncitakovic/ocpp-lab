package dev.ocpplab.billing

import com.fasterxml.jackson.databind.ObjectMapper
import dev.ocpplab.KafkaIntegrationTest
import dev.ocpplab.KafkaTestSupport
import dev.ocpplab.events.EventEnvelope
import dev.ocpplab.events.SESSION_CLOSED
import dev.ocpplab.events.SessionClosed
import dev.ocpplab.session.SessionService
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails
import org.springframework.kafka.core.KafkaTemplate
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Billing consumer against real Postgres and Kafka. Messages are produced directly, not via the outbox. */
@KafkaIntegrationTest
class BillingIntegrationTest @Autowired constructor(
    private val billing: BillingService,
    private val billingRecords: BillingRecordRepository,
    private val processedEvents: ProcessedEventRepository,
    private val sessions: SessionService,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val mapper: ObjectMapper,
    connection: KafkaConnectionDetails,
    @param:Value("\${outbox.topic}") private val topic: String,
) {
    private val kafka = KafkaTestSupport(connection.bootstrapServers, topic)
    private val startedAt = Instant.parse("2026-11-20T09:00:00Z")
    private val stoppedAt = Instant.parse("2026-11-20T10:00:00Z")

    /** A fresh transactionId (from the SERIAL), so tests never share a billing_record. */
    private fun newTransactionId(): Int = sessions.startSession("CP001", 1, "TAG-1", 1000, startedAt).transactionId!!

    private fun closed(transactionId: Int, meterStart: Int = 1000, meterStop: Int = 5500) = SessionClosed(
        transactionId = transactionId,
        chargePointId = "CP001",
        connectorId = 1,
        idTag = "TAG-1",
        meterStart = meterStart,
        meterStop = meterStop,
        startedAt = startedAt,
        stoppedAt = stoppedAt,
        reason = "Local",
    )

    /** Produces a SessionClosed envelope the way the relay does, and waits for the broker ack. */
    private fun send(eventId: UUID, event: SessionClosed) {
        val envelope = EventEnvelope(
            eventId = eventId,
            type = SESSION_CLOSED,
            aggregateId = event.transactionId.toString(),
            createdAt = stoppedAt,
            payload = mapper.valueToTree(event),
        )
        kafkaTemplate.send(topic, envelope.aggregateId, mapper.writeValueAsString(envelope)).get(10, TimeUnit.SECONDS)
    }

    @Test
    fun `bill stores energy and amount`() {
        val id = newTransactionId()

        billing.bill(UUID.randomUUID(), closed(id, meterStart = 1000, meterStop = 5500))

        val record = billingRecords.findByTransactionId(id).single()
        assertEquals(4500L, record.energyWh)
        assertEquals(0, BigDecimal("0.30").compareTo(record.pricePerKwh))
        assertEquals(BigDecimal("1.3500"), record.amount) // 4.5 kWh * 0.30
    }

    @Test
    fun `same event billed twice is billed once, the second time AlreadyProcessed`() {
        val id = newTransactionId()
        val eventId = UUID.randomUUID()

        assertEquals(BillingOutcome.Billed, billing.bill(eventId, closed(id)))
        assertEquals(BillingOutcome.AlreadyProcessed, billing.bill(eventId, closed(id)))

        assertEquals(1, billingRecords.findByTransactionId(id).size)
        assertTrue(processedEvents.exists(eventId))
    }

    @Test
    fun `same message delivered twice gives one billing record`() {
        val id = newTransactionId()
        val eventId = UUID.randomUUID()

        send(eventId, closed(id))
        send(eventId, closed(id))
        kafka.awaitConsumerGroupCaughtUp(BillingListener.GROUP_ID)

        assertEquals(1, billingRecords.findByTransactionId(id).size)
        assertTrue(processedEvents.exists(eventId))
    }

    @Test
    fun `two different event ids for the same transaction give one billing record, and the consumer keeps going`() {
        val id = newTransactionId()
        val first = UUID.randomUUID()
        val second = UUID.randomUUID()
        val next = newTransactionId()

        send(first, closed(id))
        send(second, closed(id)) // hits UNIQUE (transaction_id): must not become a poison message
        send(UUID.randomUUID(), closed(next))
        kafka.awaitConsumerGroupCaughtUp(BillingListener.GROUP_ID)

        assertEquals(first, billingRecords.findByTransactionId(id).single().eventId)
        assertTrue(processedEvents.exists(second), "the duplicate is recorded as processed")
        assertEquals(1, billingRecords.findByTransactionId(next).size, "later messages are still billed")
    }
}
