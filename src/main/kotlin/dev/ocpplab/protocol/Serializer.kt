package dev.ocpplab.protocol

import com.fasterxml.jackson.databind.ObjectMapper

/** Serializes a message into its OCPP-J JSON array form. */
fun OcppMessage.toJson(mapper: ObjectMapper): String {
    val array = mapper.createArrayNode()
    when (this) {
        is Call -> array.add(MessageTypeId.CALL).add(id).add(action).add(payload)
        is CallResult -> array.add(MessageTypeId.CALL_RESULT).add(id).add(payload)
        is CallError -> array.add(MessageTypeId.CALL_ERROR).add(id).add(errorCode.name).add(description).add(details)
    }
    return mapper.writeValueAsString(array)
}
