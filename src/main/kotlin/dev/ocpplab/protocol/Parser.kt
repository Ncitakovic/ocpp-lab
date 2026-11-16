package dev.ocpplab.protocol

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

/**
 * Parses a raw OCPP-J text frame into an [OcppMessage].
 *
 * @throws OcppParseException if the frame is not a valid OCPP-J message.
 */
fun parse(raw: String): OcppMessage {
    TODO("Nikola implementira")
}

/** Seam so the dispatcher can be tested independently of [parse]. */
fun interface MessageParser {
    fun parse(raw: String): OcppMessage
}
