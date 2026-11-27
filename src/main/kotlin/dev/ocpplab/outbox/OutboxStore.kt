package dev.ocpplab.outbox

import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

/** One row of outbox_event. [payload] is the event as JSON text. */
data class OutboxEvent(
    val id: UUID,
    val seq: Long,
    val aggregateId: String,
    val type: String,
    val payload: String,
    val createdAt: Instant,
    val publishedAt: Instant?,
)

/**
 * The two operations OutboxRelay needs. An interface so a test can wrap the real store
 * and simulate a crash between "sent to Kafka" and "marked as published".
 */
interface OutboxStore {
    /** Unpublished rows, oldest first (by seq). */
    fun findUnpublished(limit: Int): List<OutboxEvent>

    fun markPublished(id: UUID, publishedAt: Instant)
}

/**
 * JdbcClient instead of a Spring Data repository: the id is a UUID we set ourselves, and
 * Spring Data JDBC would treat an entity with a non-null id as existing and run an UPDATE.
 * Every call joins the caller's transaction if there is one (that is what makes the outbox work).
 */
@Repository
class JdbcOutboxStore(private val jdbc: JdbcClient) : OutboxStore {

    fun insert(id: UUID, aggregateId: String, type: String, payload: String, createdAt: Instant) {
        jdbc.sql(
            """
            INSERT INTO outbox_event (id, aggregate_id, type, payload, created_at)
            VALUES (:id, :aggregateId, :type, CAST(:payload AS jsonb), :createdAt)
            """,
        )
            .param("id", id)
            .param("aggregateId", aggregateId)
            .param("type", type)
            .param("payload", payload)
            .param("createdAt", Timestamp.from(createdAt))
            .update()
    }

    override fun findUnpublished(limit: Int): List<OutboxEvent> =
        jdbc.sql("SELECT * FROM outbox_event WHERE published_at IS NULL ORDER BY seq LIMIT :limit")
            .param("limit", limit)
            .query(rowMapper)
            .list()

    override fun markPublished(id: UUID, publishedAt: Instant) {
        jdbc.sql("UPDATE outbox_event SET published_at = :publishedAt WHERE id = :id AND published_at IS NULL")
            .param("id", id)
            .param("publishedAt", Timestamp.from(publishedAt))
            .update()
    }

    fun findByAggregateId(aggregateId: String): List<OutboxEvent> =
        jdbc.sql("SELECT * FROM outbox_event WHERE aggregate_id = :aggregateId ORDER BY seq")
            .param("aggregateId", aggregateId)
            .query(rowMapper)
            .list()

    private val rowMapper = RowMapper { rs, _ ->
        OutboxEvent(
            id = rs.getObject("id", UUID::class.java),
            seq = rs.getLong("seq"),
            aggregateId = rs.getString("aggregate_id"),
            type = rs.getString("type"),
            payload = rs.getString("payload"),
            createdAt = rs.getTimestamp("created_at").toInstant(),
            publishedAt = rs.getTimestamp("published_at")?.toInstant(),
        )
    }
}
