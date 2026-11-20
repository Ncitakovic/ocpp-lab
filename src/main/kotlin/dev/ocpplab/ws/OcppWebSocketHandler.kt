package dev.ocpplab.ws

import com.fasterxml.jackson.databind.ObjectMapper
import dev.ocpplab.dispatch.MessageDispatcher
import dev.ocpplab.protocol.toJson
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.handler.TextWebSocketHandler

@Component
class OcppWebSocketHandler(
    private val dispatcher: MessageDispatcher,
    private val mapper: ObjectMapper,
) : TextWebSocketHandler() {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun afterConnectionEstablished(session: WebSocketSession) {
        val cpId = session.chargePointId
        if (session.acceptedProtocol != OCPP16_SUBPROTOCOL) {
            log.warn(
                "[{}] Client did not offer subprotocol '{}', closing (session {})",
                cpId, OCPP16_SUBPROTOCOL, session.id,
            )
            session.close(CloseStatus.PROTOCOL_ERROR)
            return
        }
        log.info("[{}] Connected from {} (session {})", cpId, session.remoteAddress, session.id)
    }

    override fun handleTextMessage(session: WebSocketSession, message: TextMessage) {
        val cpId = session.chargePointId
        val raw = message.payload
        log.info("[{}] <<< {}", cpId, raw)

        val response = dispatcher.dispatch(cpId, raw) ?: return

        val out = response.toJson(mapper)
        log.info("[{}] >>> {}", cpId, out)
        session.sendMessage(TextMessage(out))
    }

    override fun handleTransportError(session: WebSocketSession, exception: Throwable) {
        log.warn("[{}] Transport error: {}", session.chargePointId, exception.message)
    }

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        log.info("[{}] Disconnected: {} (session {})", session.chargePointId, status, session.id)
    }

    private val WebSocketSession.chargePointId: String
        get() = attributes[ChargePointIdInterceptor.CHARGE_POINT_ID] as? String ?: "?"
}
