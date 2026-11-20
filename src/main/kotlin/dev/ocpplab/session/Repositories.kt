package dev.ocpplab.session

import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.query.Param
import java.time.Instant

interface ChargingSessionRepository : CrudRepository<ChargingSession, Int> {

    /**
     * Closes the session only if it is still OPEN, in a single conditional UPDATE.
     *
     * @return number of updated rows: 1 if this call closed the session,
     *         0 if it was already closed or does not exist.
     *
     * Replace the body with `@Modifying @Query("UPDATE charging_session SET ... WHERE ...")`
     * on this declaration and delete the body. Spring Data skips default methods
     * when it derives queries, so it calls this body as long as it exists.
     */
    fun closeIfOpen(
        @Param("transactionId") transactionId: Int,
        @Param("meterStop") meterStop: Int,
        @Param("stoppedAt") stoppedAt: Instant,
        @Param("reason") reason: String,
    ): Int {
        TODO("Nikola implementira")
    }
}

interface MeterValueRepository : CrudRepository<MeterValue, Long> {
    fun findByTransactionIdOrderByTimestamp(transactionId: Int): List<MeterValue>
}
