package dev.ocpplab.session

import dev.ocpplab.events.SessionOpened
import dev.ocpplab.outbox.Outbox
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.time.Instant

/**
 * @Transactional works through the Spring proxy, so it only applies when these methods are
 * called from another bean (MessageDispatcher), never via `this`.
 */
@Service
class JdbcSessionService(
    private val sessions: ChargingSessionRepository,
    private val meterValues: MeterValueRepository,
    private val outbox: Outbox,
) : SessionService {

    private val log = LoggerFactory.getLogger(javaClass)

    /** The session row and its SessionOpened outbox row commit together, or not at all. */
    @Transactional
    override fun startSession(
        chargePointId: String,
        connectorId: Int,
        idTag: String,
        meterStart: Int,
        startedAt: Instant,
    ): ChargingSession {
        val saved = sessions.save(
            ChargingSession(
                chargePointId = chargePointId,
                connectorId = connectorId,
                idTag = idTag,
                meterStart = meterStart,
                startedAt = startedAt,
            ),
        )
        outbox.append(
            SessionOpened(
                transactionId = saved.transactionId!!,
                chargePointId = chargePointId,
                connectorId = connectorId,
                idTag = idTag,
                meterStart = meterStart,
                startedAt = startedAt,
            ),
        )
        log.info("[{}] Session {} opened on connector {} for {}", chargePointId, saved.transactionId, connectorId, idTag)
        return saved
    }

    override fun recordMeterValue(transactionId: Int, timestamp: Instant, valueWh: BigDecimal): Boolean {
        if (!sessions.existsById(transactionId)) return false
        meterValues.save(MeterValue(transactionId = transactionId, timestamp = timestamp, valueWh = valueWh))
        return true
    }

    // TODO (a) [user]: write the SessionClosed event to the outbox in the SAME transaction as closeIfOpen.
    //
    //  1. Put @Transactional on this method (already imported). outbox.append(...) uses
    //     Propagation.MANDATORY, so without a transaction it throws IllegalTransactionStateException.
    //  2. Only when updated == 1 (this call closed the session):
    //       - load the session with sessions.findById(transactionId). Inside the same transaction it
    //         already shows the new values (status CLOSED, meterStop, stoppedAt, stopReason).
    //       - call outbox.append(SessionClosed(...)) (dev.ocpplab.events.SessionClosed). Fields:
    //         transactionId, chargePointId, connectorId, idTag, meterStart, meterStop, startedAt,
    //         stoppedAt, reason.
    //  3. AlreadyClosed and UnknownTransaction must NOT write an outbox row.
    //
    //  Tests that check this: OutboxKafkaIntegrationTest (closing writes exactly one event,
    //  concurrent stops write one row, crash before published_at).
    override fun stopSession(transactionId: Int, meterStop: Int, stoppedAt: Instant, reason: String): StopOutcome {
        val updated = sessions.closeIfOpen(transactionId, meterStop, stoppedAt, reason)
        return when {
            updated == 1 -> StopOutcome.Closed
            sessions.existsById(transactionId) -> StopOutcome.AlreadyClosed
            else -> StopOutcome.UnknownTransaction
        }
    }
}
