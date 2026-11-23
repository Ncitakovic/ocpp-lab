package dev.ocpplab.dispatch

import java.time.Instant

// OCPP 1.6 Core request payloads (only the fields the CSMS uses; unknown fields are ignored).
// Non-nullable properties are required by the spec; a missing one is a FormationViolation.

data class AuthorizeRequest(
    val idTag: String,
)

data class StatusNotificationRequest(
    val connectorId: Int,
    val errorCode: String,
    val status: String,
    val timestamp: Instant? = null,
    val info: String? = null,
)

data class StartTransactionRequest(
    val connectorId: Int,
    val idTag: String,
    val meterStart: Int,
    val timestamp: Instant,
    val reservationId: Int? = null,
)

data class MeterValuesRequest(
    val connectorId: Int,
    val transactionId: Int? = null,
    val meterValue: List<MeterValueSample>,
)

data class MeterValueSample(
    val timestamp: Instant,
    val sampledValue: List<SampledValue>,
)

data class SampledValue(
    val value: String,
    val measurand: String? = null,
    val unit: String? = null,
    val phase: String? = null,
    val context: String? = null,
)

data class StopTransactionRequest(
    val transactionId: Int,
    val meterStop: Int,
    val timestamp: Instant,
    val idTag: String? = null,
    val reason: String? = null,
)
