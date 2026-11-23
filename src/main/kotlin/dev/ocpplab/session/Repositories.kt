package dev.ocpplab.session

import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.query.Param
import org.springframework.data.jdbc.repository.query.Modifying
import org.springframework.data.jdbc.repository.query.Query
import java.time.Instant

interface ChargingSessionRepository : CrudRepository<ChargingSession, Int> {

    /**
     * Closes the session only if it is still OPEN, in a single conditional UPDATE.
     *
     * @return number of updated rows: 1 if this call closed the session,
     *         0 if it was already closed or does not exist.
     */
    @Modifying
    @Query("""
        UPDATE charging_session
        SET status = 'CLOSED', meter_stop = :meterStop,
            stopped_at = :stoppedAt, stop_reason = :reason
        WHERE transaction_id = :transactionId AND status = 'OPEN'
    """)
    fun closeIfOpen(
        @Param("transactionId") transactionId: Int,
        @Param("meterStop") meterStop: Int,
        @Param("stoppedAt") stoppedAt: Instant,
        @Param("reason") reason: String,
    ): Int
}

interface MeterValueRepository : CrudRepository<MeterValue, Long> {
    fun findByTransactionIdOrderByTimestamp(transactionId: Int): List<MeterValue>
}
