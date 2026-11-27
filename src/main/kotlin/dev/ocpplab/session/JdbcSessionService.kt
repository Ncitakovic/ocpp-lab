package dev.ocpplab.session

import dev.ocpplab.events.SessionClosed
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

    /**
     * Closing the session and writing its SessionClosed outbox row are one transaction: both commit,
     * or neither. Only the call that actually closed the session (updated == 1) writes the event,
     * so a duplicate or unknown StopTransaction never produces one.
     */
    @Transactional
    override fun stopSession(transactionId: Int, meterStop: Int, stoppedAt: Instant, reason: String): StopOutcome {
        val updated = sessions.closeIfOpen(transactionId, meterStop, stoppedAt, reason)
        return when {
            updated == 1 -> {
                // Same transaction, so this already sees the row closeIfOpen just updated.
                val session = sessions.findById(transactionId).orElseThrow()
                outbox.append(
                    SessionClosed(
                        transactionId = transactionId,
                        chargePointId = session.chargePointId,
                        connectorId = session.connectorId,
                        idTag = session.idTag,
                        meterStart = session.meterStart,
                        meterStop = meterStop,
                        startedAt = session.startedAt,
                        stoppedAt = stoppedAt,
                        reason = reason,
                    ),
                )
                StopOutcome.Closed
            }
            sessions.existsById(transactionId) -> StopOutcome.AlreadyClosed
            else -> StopOutcome.UnknownTransaction
        }
    }
}
