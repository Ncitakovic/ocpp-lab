package dev.ocpplab.dispatch

import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

data class ConnectorStatus(
    val status: String,
    val errorCode: String,
    val timestamp: Instant,
)

/** Last reported StatusNotification per (chargePointId, connectorId). In memory only. */
@Component
class ConnectorStatusRegistry {

    private val statuses = ConcurrentHashMap<Pair<String, Int>, ConnectorStatus>()

    fun update(chargePointId: String, connectorId: Int, status: ConnectorStatus) {
        statuses[chargePointId to connectorId] = status
    }

    fun get(chargePointId: String, connectorId: Int): ConnectorStatus? = statuses[chargePointId to connectorId]
}
