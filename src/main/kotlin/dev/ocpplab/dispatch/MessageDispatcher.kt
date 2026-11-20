package dev.ocpplab.dispatch

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import dev.ocpplab.protocol.Call
import dev.ocpplab.protocol.CallError
import dev.ocpplab.protocol.CallResult
import dev.ocpplab.protocol.MessageParser
import dev.ocpplab.protocol.OcppErrorCode
import dev.ocpplab.protocol.OcppMessage
import dev.ocpplab.protocol.OcppParseException
import dev.ocpplab.session.SessionService
import dev.ocpplab.session.StopOutcome
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

/**
 * Turns a raw incoming frame into the response that should be sent back
 * (or null if nothing should be sent).
 */
@Component
class MessageDispatcher(
    private val clock: Clock,
    private val parser: MessageParser,
    private val mapper: ObjectMapper,
    private val sessions: SessionService,
    private val connectorStatuses: ConnectorStatusRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val json = JsonNodeFactory.instance

    // Without this a missing required number (e.g. meterStart) silently becomes 0.
    private val payloadReader = mapper.reader().with(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)

    fun dispatch(chargePointId: String, raw: String): OcppMessage? {
        val message = try {
            parser.parse(raw)
        } catch (e: OcppParseException) {
            return onParseError(chargePointId, e)
        }

        return when (message) {
            is Call -> handleCall(chargePointId, message)
            is CallResult, is CallError -> {
                // The CSMS does not send its own CALLs yet, so there is nothing to correlate these with.
                log.info("[{}] Ignoring {} for id={}", chargePointId, message::class.simpleName, message.id)
                null
            }
        }
    }

    private fun onParseError(chargePointId: String, e: OcppParseException): OcppMessage? {
        val id = e.messageId
        if (id == null) {
            log.warn("[{}] Unparseable frame without readable id, dropping: {}", chargePointId, e.message)
            return null
        }
        log.warn("[{}] Malformed message id={}: {}", chargePointId, id, e.message)
        return CallError(id, OcppErrorCode.FormationViolation, e.message ?: "Malformed message", emptyObject())
    }

    private fun handleCall(chargePointId: String, call: Call): OcppMessage =
        try {
            when (call.action) {
                "BootNotification" -> bootNotification(chargePointId, call)
                "Heartbeat" -> CallResult(call.id, emptyObject().put("currentTime", now()))
                "Authorize" -> authorize(chargePointId, call)
                "StatusNotification" -> statusNotification(chargePointId, call)
                "StartTransaction" -> startTransaction(chargePointId, call)
                "MeterValues" -> meterValues(chargePointId, call)
                "StopTransaction" -> stopTransaction(chargePointId, call)
                else -> {
                    log.warn("[{}] Unsupported action '{}'", chargePointId, call.action)
                    CallError(call.id, OcppErrorCode.NotImplemented, "Unknown action: ${call.action}", emptyObject())
                }
            }
        } catch (e: InvalidPayloadException) {
            log.warn("[{}] Invalid {} payload: {}", chargePointId, call.action, e.message)
            CallError(call.id, OcppErrorCode.FormationViolation, e.message ?: "Invalid payload", emptyObject())
        } catch (e: Exception) {
            log.error("[{}] Failed to handle {} id={}", chargePointId, call.action, call.id, e)
            CallError(call.id, OcppErrorCode.InternalError, "Internal error while handling ${call.action}", emptyObject())
        }

    private fun bootNotification(chargePointId: String, call: Call): CallResult {
        log.info("[{}] BootNotification: {}", chargePointId, call.payload)
        return CallResult(
            call.id,
            emptyObject()
                .put("status", "Accepted")
                .put("currentTime", now())
                .put("interval", HEARTBEAT_INTERVAL_SECONDS),
        )
    }

    private fun authorize(chargePointId: String, call: Call): CallResult {
        val req = call.payloadAs<AuthorizeRequest>()
        log.info("[{}] Authorize idTag={} -> Accepted", chargePointId, req.idTag)
        return CallResult(call.id, emptyObject().set(ID_TAG_INFO, accepted()))
    }

    private fun statusNotification(chargePointId: String, call: Call): CallResult {
        val req = call.payloadAs<StatusNotificationRequest>()
        val status = ConnectorStatus(req.status, req.errorCode, req.timestamp ?: Instant.now(clock))
        connectorStatuses.update(chargePointId, req.connectorId, status)
        log.info("[{}] Connector {} is {} (errorCode={})", chargePointId, req.connectorId, req.status, req.errorCode)
        return CallResult(call.id, emptyObject())
    }

    private fun startTransaction(chargePointId: String, call: Call): CallResult {
        val req = call.payloadAs<StartTransactionRequest>()
        val session = sessions.startSession(chargePointId, req.connectorId, req.idTag, req.meterStart, req.timestamp)
        return CallResult(
            call.id,
            emptyObject()
                .put("transactionId", session.transactionId)
                .set(ID_TAG_INFO, accepted()),
        )
    }

    private fun meterValues(chargePointId: String, call: Call): CallResult {
        val req = call.payloadAs<MeterValuesRequest>()
        val transactionId = req.transactionId
        if (transactionId == null) {
            log.info("[{}] MeterValues without transactionId on connector {}, not stored", chargePointId, req.connectorId)
            return CallResult(call.id, emptyObject())
        }

        for (sample in req.meterValue) {
            for (sv in sample.sampledValue) {
                val wh = energyImportWh(sv) ?: continue
                if (!sessions.recordMeterValue(transactionId, sample.timestamp, wh)) {
                    log.warn("[{}] MeterValues for unknown transaction {}, not stored", chargePointId, transactionId)
                    return CallResult(call.id, emptyObject())
                }
                log.info("[{}] Transaction {}: {} Wh at {}", chargePointId, transactionId, wh, sample.timestamp)
            }
        }
        return CallResult(call.id, emptyObject())
    }

    private fun stopTransaction(chargePointId: String, call: Call): CallResult {
        val req = call.payloadAs<StopTransactionRequest>()
        val reason = req.reason ?: "Local" // spec: reason may only be omitted when it is "Local"

        when (sessions.stopSession(req.transactionId, req.meterStop, req.timestamp, reason)) {
            StopOutcome.Closed ->
                log.info("[{}] Transaction {} closed, meterStop={} reason={}", chargePointId, req.transactionId, req.meterStop, reason)
            StopOutcome.AlreadyClosed ->
                log.warn("[{}] Duplicate StopTransaction for already closed transaction {}", chargePointId, req.transactionId)
            StopOutcome.UnknownTransaction ->
                log.warn("[{}] StopTransaction for unknown transaction {}", chargePointId, req.transactionId)
        }

        // Always a normal CallResult: otherwise the charge point keeps retrying the StopTransaction.
        val payload = emptyObject()
        if (req.idTag != null) payload.set<JsonNode>(ID_TAG_INFO, accepted())
        return CallResult(call.id, payload)
    }

    /**
     * Returns the energy in Wh if this sampled value is the total imported energy register,
     * otherwise null. Per-phase values are skipped so the total is not counted several times.
     */
    private fun energyImportWh(sv: SampledValue): BigDecimal? {
        val measurand = sv.measurand ?: ENERGY_IMPORT_REGISTER // spec default
        if (measurand != ENERGY_IMPORT_REGISTER || sv.phase != null) return null

        val value = sv.value.toBigDecimalOrNull() ?: throw InvalidPayloadException("sampledValue.value '${sv.value}' is not a number")
        return when (sv.unit ?: "Wh") {
            "Wh" -> value
            "kWh" -> value.multiply(BigDecimal(1000))
            else -> throw InvalidPayloadException("Unsupported unit '${sv.unit}' for $ENERGY_IMPORT_REGISTER")
        }
    }

    private inline fun <reified T> Call.payloadAs(): T =
        try {
            payloadReader.treeToValue(payload, T::class.java)
        } catch (e: JsonProcessingException) {
            throw InvalidPayloadException("Invalid $action payload: ${e.originalMessage}")
        } catch (e: IllegalArgumentException) {
            throw InvalidPayloadException("Invalid $action payload: ${e.message}")
        }

    private class InvalidPayloadException(message: String) : RuntimeException(message)

    /** Current UTC time as ISO-8601 with millisecond precision, e.g. 2026-11-16T10:15:30.123Z. */
    private fun now(): String =
        DateTimeFormatter.ISO_INSTANT.format(Instant.now(clock).truncatedTo(ChronoUnit.MILLIS))

    private fun emptyObject(): ObjectNode = json.objectNode()

    private fun accepted(): ObjectNode = emptyObject().put("status", "Accepted")

    companion object {
        const val HEARTBEAT_INTERVAL_SECONDS = 60
        const val ENERGY_IMPORT_REGISTER = "Energy.Active.Import.Register"
        private const val ID_TAG_INFO = "idTagInfo"
    }
}
