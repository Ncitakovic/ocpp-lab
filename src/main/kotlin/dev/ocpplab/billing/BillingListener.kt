package dev.ocpplab.billing

import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.databind.ObjectMapper
import dev.ocpplab.events.EventEnvelope
import dev.ocpplab.events.SESSION_CLOSED
import dev.ocpplab.events.SessionClosed
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

/**
 * Billing consumer: at-least-once, offset committed manually (ack-mode manual_immediate)
 * only after the database transaction in BillingService has committed.
 *
 * Crash after commit, before ack: the message comes again, and processed_event turns it into a no-op.
 * Crash before commit: nothing was written, and the message comes again and is billed then.
 */
@Component
class BillingListener(
    private val billing: BillingService,
    private val mapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["\${outbox.topic}"], groupId = GROUP_ID)
    fun onMessage(record: ConsumerRecord<String, String>, ack: Acknowledgment) {
        val (envelope, event) = try {
            val envelope = mapper.readValue(record.value(), EventEnvelope::class.java)
            if (envelope.type != SESSION_CLOSED) {
                ack.acknowledge() // billing only cares about closed sessions
                return
            }
            envelope to mapper.treeToValue(envelope.payload, SessionClosed::class.java)
        } catch (e: JacksonException) {
            // Will never parse, however often it is retried: skip it instead of blocking the partition.
            log.error("Unreadable message at {}-{}@{}, skipped: {}", record.topic(), record.partition(), record.offset(), e.toString())
            ack.acknowledge()
            return
        }

        // Duplicates (same event id, or a session already billed by another event id) come back as
        // AlreadyProcessed, not as an exception, so they are acknowledged and never block the partition.
        when (billing.bill(envelope.eventId, event)) {
            BillingOutcome.Billed ->
                log.info("Transaction {} billed (event {})", event.transactionId, envelope.eventId)
            BillingOutcome.AlreadyProcessed ->
                log.info("Event {} already processed, skipped", envelope.eventId)
        }
        // bill() has returned, so its transaction has committed: only now is it safe to commit the offset.
        ack.acknowledge()
    }

    companion object {
        const val GROUP_ID = "billing"
    }
}
