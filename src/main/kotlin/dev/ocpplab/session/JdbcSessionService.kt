package dev.ocpplab.session

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.time.Instant

@Service
class JdbcSessionService(
    private val sessions: ChargingSessionRepository,
    private val meterValues: MeterValueRepository,
) : SessionService {

    private val log = LoggerFactory.getLogger(javaClass)

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
        log.info("[{}] Session {} opened on connector {} for {}", chargePointId, saved.transactionId, connectorId, idTag)
        return saved
    }

    override fun recordMeterValue(transactionId: Int, timestamp: Instant, valueWh: BigDecimal): Boolean {
        if (!sessions.existsById(transactionId)) return false
        meterValues.save(MeterValue(transactionId = transactionId, timestamp = timestamp, valueWh = valueWh))
        return true
    }

    override fun stopSession(transactionId: Int, meterStop: Int, stoppedAt: Instant, reason: String): StopOutcome {
        TODO("Nikola implementira")
    }
}
