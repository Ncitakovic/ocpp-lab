package dev.ocpplab.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import dev.ocpplab.events.EventEnvelope
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.time.Clock
import java.util.concurrent.TimeUnit

/**
 * Publishes unpublished outbox rows to Kafka, oldest first.
 *
 * For each row: send, wait for the broker ack (acks=all) with a timeout, and only then set
 * published_at. If the process dies between the ack and the UPDATE, the row is still
 * unpublished and is sent again on the next run: at-least-once, never lost. The consumer
 * deduplicates by eventId.
 *
 * On the first failed send the batch stops, so a later row never overtakes an earlier one.
 *
 * Runs on a single instance (see README "Limitations"). Called by OutboxRelayScheduler,
 * and directly from tests.
 */
@Component
class OutboxRelay(
    private val store: OutboxStore,
    private val kafka: KafkaTemplate<String, String>,
    private val mapper: ObjectMapper,
    private val clock: Clock,
    @param:Value("\${outbox.topic}") private val topic: String,
    @param:Value("\${outbox.relay.batch-size}") private val batchSize: Int,
    @param:Value("\${outbox.relay.send-timeout-ms}") private val sendTimeoutMs: Long,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** @return how many rows were published in this run. */
    fun publishPending(): Int {
        var published = 0
        for (event in store.findUnpublished(batchSize)) {
            val record = ProducerRecord(topic, event.aggregateId, toEnvelopeJson(event))
            try {
                kafka.send(record).get(sendTimeoutMs, TimeUnit.MILLISECONDS)
            } catch (e: Exception) {
                if (e is InterruptedException) Thread.currentThread().interrupt()
                log.warn("Outbox event {} ({}) not sent, will retry: {}", event.id, event.type, e.toString())
                break
            }
            // A crash right here (sent, but published_at not written) means a resend: at-least-once.
            store.markPublished(event.id, clock.instant())
            published++
            log.debug("Outbox event {} {} for {} published", event.id, event.type, event.aggregateId)
        }
        return published
    }

    private fun toEnvelopeJson(event: OutboxEvent): String =
        mapper.writeValueAsString(
            EventEnvelope(
                eventId = event.id,
                type = event.type,
                aggregateId = event.aggregateId,
                createdAt = event.createdAt,
                payload = mapper.readTree(event.payload),
            ),
        )
}
