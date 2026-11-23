package dev.ocpplab.billing

import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

data class BillingRecord(
    val id: Long? = null,
    val transactionId: Int,
    val eventId: UUID,
    val chargePointId: String,
    val idTag: String,
    val energyWh: Long,
    val pricePerKwh: BigDecimal,
    val amount: BigDecimal,
    val billedAt: Instant,
)

@Repository
class ProcessedEventRepository(private val jdbc: JdbcClient) {

    fun exists(eventId: UUID): Boolean =
        jdbc.sql("SELECT EXISTS (SELECT 1 FROM processed_event WHERE event_id = :eventId)")
            .param("eventId", eventId)
            .query(Boolean::class.javaObjectType)
            .single()

    /** @return 1 if the row was inserted, 0 if this eventId was already there. Never throws on a duplicate. */
    fun insertIfAbsent(eventId: UUID, processedAt: Instant): Int =
        jdbc.sql(
            """
            INSERT INTO processed_event (event_id, processed_at) VALUES (:eventId, :processedAt)
            ON CONFLICT (event_id) DO NOTHING
            """,
        )
            .param("eventId", eventId)
            .param("processedAt", Timestamp.from(processedAt))
            .update()
}

@Repository
class BillingRecordRepository(private val jdbc: JdbcClient) {

    /** Throws DuplicateKeyException if this transactionId is already billed (UNIQUE). */
    fun insert(record: BillingRecord) {
        jdbc.sql(
            """
            INSERT INTO billing_record
                (transaction_id, event_id, charge_point_id, id_tag, energy_wh, price_per_kwh, amount, billed_at)
            VALUES
                (:transactionId, :eventId, :chargePointId, :idTag, :energyWh, :pricePerKwh, :amount, :billedAt)
            """,
        )
            .param("transactionId", record.transactionId)
            .param("eventId", record.eventId)
            .param("chargePointId", record.chargePointId)
            .param("idTag", record.idTag)
            .param("energyWh", record.energyWh)
            .param("pricePerKwh", record.pricePerKwh)
            .param("amount", record.amount)
            .param("billedAt", Timestamp.from(record.billedAt))
            .update()
    }

    fun findByTransactionId(transactionId: Int): List<BillingRecord> =
        jdbc.sql("SELECT * FROM billing_record WHERE transaction_id = :transactionId")
            .param("transactionId", transactionId)
            .query(rowMapper)
            .list()

    private val rowMapper = RowMapper { rs, _ ->
        BillingRecord(
            id = rs.getLong("id"),
            transactionId = rs.getInt("transaction_id"),
            eventId = rs.getObject("event_id", UUID::class.java),
            chargePointId = rs.getString("charge_point_id"),
            idTag = rs.getString("id_tag"),
            energyWh = rs.getLong("energy_wh"),
            pricePerKwh = rs.getBigDecimal("price_per_kwh"),
            amount = rs.getBigDecimal("amount"),
            billedAt = rs.getTimestamp("billed_at").toInstant(),
        )
    }
}
