package dev.ocpplab.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import dev.ocpplab.events.SessionEvent
import dev.ocpplab.events.typeName
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
import java.util.UUID

/** Writes domain events to the outbox table. Nothing is sent to Kafka here; OutboxRelay does that. */
@Component
class Outbox(
    private val store: JdbcOutboxStore,
    private val mapper: ObjectMapper,
    private val clock: Clock,
) {
    /**
     * Inserts [event] as an outbox row and returns its event id.
     *
     * MANDATORY: there must already be a transaction (the one that changes the session).
     * Without one Spring throws IllegalTransactionStateException instead of silently
     * committing the outbox row on its own, which would defeat the whole pattern.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    fun append(event: SessionEvent): UUID {
        val id = UUID.randomUUID()
        store.insert(
            id = id,
            aggregateId = event.transactionId.toString(),
            type = event.typeName(),
            payload = mapper.writeValueAsString(event),
            createdAt = clock.instant(),
        )
        return id
    }
}
