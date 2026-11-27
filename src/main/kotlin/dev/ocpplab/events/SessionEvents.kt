package dev.ocpplab.events

import com.fasterxml.jackson.databind.JsonNode
import java.time.Instant
import java.util.UUID

const val SESSION_OPENED = "SessionOpened"
const val SESSION_CLOSED = "SessionClosed"

/** Domain events about a charging session. Stored in the outbox as JSON and published to Kafka. */
sealed interface SessionEvent {
    val transactionId: Int
}

data class SessionOpened(
    override val transactionId: Int,
    val chargePointId: String,
    val connectorId: Int,
    val idTag: String,
    val meterStart: Int,
    val startedAt: Instant,
) : SessionEvent

data class SessionClosed(
    override val transactionId: Int,
    val chargePointId: String,
    val connectorId: Int,
    val idTag: String,
    val meterStart: Int,
    val meterStop: Int,
    val startedAt: Instant,
    val stoppedAt: Instant,
    val reason: String,
) : SessionEvent

fun SessionEvent.typeName(): String = when (this) {
    is SessionOpened -> SESSION_OPENED
    is SessionClosed -> SESSION_CLOSED
}

/**
 * What actually goes over Kafka (the record value). [eventId] is the outbox row id and stays
 * the same when the relay sends the row again, so the consumer can recognise a redelivery.
 */
data class EventEnvelope(
    val eventId: UUID,
    val type: String,
    val aggregateId: String,
    val createdAt: Instant,
    val payload: JsonNode,
)
