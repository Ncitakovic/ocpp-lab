package dev.ocpplab.protocol

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

/**
 * Thrown when a raw frame is not a valid OCPP-J message.
 *
 * [messageId] is the UniqueId if it could be read from the frame, otherwise null.
 * The dispatcher uses it to decide whether a CALLERROR can be sent back.
 */
class OcppParseException(
    val messageId: String?,
    message: String,
) : RuntimeException(message)

// Reject `[...] garbage` instead of silently ignoring what follows the first JSON value.
private val mapper = jacksonObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

/**
 * Parses a raw OCPP-J text frame into an [OcppMessage].
 *
 * @throws OcppParseException if the frame is not a valid OCPP-J message.
 */
fun parse(raw: String): OcppMessage {
    // 1. is it JSON at all?
    val node = try {
        mapper.readTree(raw)
    } catch (e: JsonProcessingException) {
        throw OcppParseException(null, "Invalid JSON: ${e.originalMessage}")
    }

    // 2. is it an array with a type and an id?
    if (!node.isArray || node.size() < 3) {
        throw OcppParseException(null, "Not an OCPP-J array")
    }
    val id = node[1].takeIf { it.isTextual }?.asText()
        ?: throw OcppParseException(null, "Message id must be a string")
    if (id.length > MAX_MESSAGE_ID_LENGTH) {
        throw OcppParseException(id, "Message id longer than $MAX_MESSAGE_ID_LENGTH characters")
    }

    // 3. based on the type, read the fields of that type
    val type = node[0].takeIf { it.isInt }?.intValue()
        ?: throw OcppParseException(id, "MessageTypeId must be an integer, got ${node[0]}")

    return when (type) {
        MessageTypeId.CALL -> {
            expectSize(node, 4, id)
            Call(
                id,
                action = text(node[2], "action", id),
                payload = obj(node[3], "payload", id),
            )
        }

        MessageTypeId.CALL_RESULT -> {
            expectSize(node, 3, id)
            CallResult(id, payload = obj(node[2], "payload", id))
        }

        MessageTypeId.CALL_ERROR -> {
            expectSize(node, 5, id)
            CallError(
                id,
                errorCode = errorCode(text(node[2], "errorCode", id), id),
                description = text(node[3], "errorDescription", id),
                details = obj(node[4], "errorDetails", id),
            )
        }

        else -> throw OcppParseException(id, "Unknown MessageTypeId $type")
    }
}

private fun expectSize(node: JsonNode, size: Int, id: String) {
    if (node.size() != size) {
        throw OcppParseException(id, "Expected $size elements, got ${node.size()}")
    }
}

private fun text(node: JsonNode, field: String, id: String): String =
    node.takeIf { it.isTextual }?.asText()
        ?: throw OcppParseException(id, "$field must be a string")

private fun obj(node: JsonNode, field: String, id: String): JsonNode =
    node.takeIf { it.isObject }
        ?: throw OcppParseException(id, "$field must be a JSON object")

private fun errorCode(code: String, id: String): OcppErrorCode =
    OcppErrorCode.entries.firstOrNull { it.name == code }
        ?: throw OcppParseException(id, "Unknown errorCode '$code'")

/** Seam so the dispatcher can be tested independently of [parse]. */
fun interface MessageParser {
    fun parse(raw: String): OcppMessage
}
