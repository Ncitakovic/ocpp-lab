package dev.ocpplab.session

import java.math.BigDecimal
import java.time.Instant

/** Result of a StopTransaction, as seen by the CSMS. */
sealed interface StopOutcome {
    /** This call closed the session. */
    data object Closed : StopOutcome

    /** The session was already closed (duplicate / retried StopTransaction). */
    data object AlreadyClosed : StopOutcome

    /** No session with this transactionId exists. */
    data object UnknownTransaction : StopOutcome
}

/**
 * Charging session lifecycle. An interface so the dispatcher can be unit-tested
 * with a fake; the real implementation is [JdbcSessionService].
 */
interface SessionService {

    fun startSession(
        chargePointId: String,
        connectorId: Int,
        idTag: String,
        meterStart: Int,
        startedAt: Instant,
    ): ChargingSession

    /** @return false if the transaction does not exist (the value is not stored). */
    fun recordMeterValue(transactionId: Int, timestamp: Instant, valueWh: BigDecimal): Boolean

    fun stopSession(transactionId: Int, meterStop: Int, stoppedAt: Instant, reason: String): StopOutcome
}
