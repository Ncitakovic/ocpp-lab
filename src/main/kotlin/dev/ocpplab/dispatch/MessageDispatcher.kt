package dev.ocpplab.dispatch

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import dev.ocpplab.protocol.Call
import dev.ocpplab.protocol.CallError
import dev.ocpplab.protocol.CallResult
import dev.ocpplab.protocol.MessageParser
import dev.ocpplab.protocol.OcppErrorCode
import dev.ocpplab.protocol.OcppMessage
import dev.ocpplab.protocol.OcppParseException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
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
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val json = JsonNodeFactory.instance

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
        return CallError(id, OcppErrorCode.FormationViolation, e.message ?: "Malformed message", emptyDetails())
    }

    private fun handleCall(chargePointId: String, call: Call): OcppMessage =
        when (call.action) {
            "BootNotification" -> {
                log.info("[{}] BootNotification: {}", chargePointId, call.payload)
                CallResult(
                    call.id,
                    json.objectNode()
                        .put("status", "Accepted")
                        .put("currentTime", now())
                        .put("interval", HEARTBEAT_INTERVAL_SECONDS),
                )
            }

            "Heartbeat" -> CallResult(call.id, json.objectNode().put("currentTime", now()))

            else -> {
                log.warn("[{}] Unsupported action '{}'", chargePointId, call.action)
                CallError(call.id, OcppErrorCode.NotImplemented, "Unknown action: ${call.action}", emptyDetails())
            }
        }

    /** Current UTC time as ISO-8601 with millisecond precision, e.g. 2026-11-16T10:15:30.123Z. */
    private fun now(): String =
        DateTimeFormatter.ISO_INSTANT.format(Instant.now(clock).truncatedTo(ChronoUnit.MILLIS))

    private fun emptyDetails(): JsonNode = json.objectNode()

    companion object {
        const val HEARTBEAT_INTERVAL_SECONDS = 60
    }
}
