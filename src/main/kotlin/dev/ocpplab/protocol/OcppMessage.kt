package dev.ocpplab.protocol

import com.fasterxml.jackson.databind.JsonNode

/**
 * OCPP-J RPC framing (OCPP-J 1.6, section 4).
 *
 *   CALL       [2, "<UniqueId>", "<Action>", {<Payload>}]
 *   CALLRESULT [3, "<UniqueId>", {<Payload>}]
 *   CALLERROR  [4, "<UniqueId>", "<errorCode>", "<errorDescription>", {<errorDetails>}]
 */
sealed interface OcppMessage {
    val id: String
}

data class Call(
    override val id: String,
    val action: String,
    val payload: JsonNode,
) : OcppMessage

data class CallResult(
    override val id: String,
    val payload: JsonNode,
) : OcppMessage

data class CallError(
    override val id: String,
    val errorCode: OcppErrorCode,
    val description: String,
    val details: JsonNode,
) : OcppMessage

/** MessageTypeId values (OCPP-J 1.6, section 4.1.3). */
object MessageTypeId {
    const val CALL = 2
    const val CALL_RESULT = 3
    const val CALL_ERROR = 4
}

/** Maximum length of the UniqueId (OCPP-J 1.6, section 4.1.4). */
const val MAX_MESSAGE_ID_LENGTH = 36
