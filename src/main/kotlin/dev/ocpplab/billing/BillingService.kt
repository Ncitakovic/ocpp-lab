package dev.ocpplab.billing

import dev.ocpplab.events.SessionClosed
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Clock
import java.util.UUID

sealed interface BillingOutcome {
    data object Billed : BillingOutcome
    data object AlreadyProcessed : BillingOutcome
}

private val WH_PER_KWH = BigDecimal(1000)

/** amount = energyWh / 1000 * pricePerKwh, rounded HALF_UP to 4 decimals (NUMERIC(12,4)). */
fun computeAmount(energyWh: Long, pricePerKwh: BigDecimal): BigDecimal =
    BigDecimal.valueOf(energyWh)
        .multiply(pricePerKwh)
        .divide(WH_PER_KWH, 4, RoundingMode.HALF_UP)

/**
 * A separate bean from BillingListener on purpose: @Transactional works through the Spring proxy,
 * so the listener must call these methods on this bean, not via `this`. When bill() returns,
 * the transaction has committed, and only then does the listener acknowledge the offset.
 */
@Service
class BillingService(
    private val processedEvents: ProcessedEventRepository,
    private val billingRecords: BillingRecordRepository,
    private val clock: Clock,
    @param:Value("\${billing.price-per-kwh}") private val pricePerKwh: BigDecimal,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Bills one closed session. processed_event and billing_record are written in ONE transaction:
     * both rows commit, or neither.
     */
    @Transactional
    fun bill(eventId: UUID, event: SessionClosed): BillingOutcome {
        val now = clock.instant()

        // Two deliveries of the same event at the same moment: exists() followed by an insert is
        // check-then-act, so both could see "not processed" and both would bill. insertIfAbsent is a
        // single atomic INSERT ... ON CONFLICT DO NOTHING: the second one waits on the first one's
        // primary-key entry and, once that commits, inserts nothing and returns 0, so exactly one bills.
        if (processedEvents.insertIfAbsent(eventId, now) == 0) {
            return BillingOutcome.AlreadyProcessed
        }

        val energyWh = event.meterStop.toLong() - event.meterStart
        val inserted = billingRecords.insertIfAbsent(
            BillingRecord(
                transactionId = event.transactionId,
                eventId = eventId,
                chargePointId = event.chargePointId,
                idTag = event.idTag,
                energyWh = energyWh,
                pricePerKwh = pricePerKwh,
                amount = computeAmount(energyWh, pricePerKwh),
                billedAt = now,
            ),
        )
        if (inserted == 0) {
            // UNIQUE (transaction_id): this session was already billed by a different event id.
            // Not an error to retry (it would never succeed): the event stays recorded as processed.
            log.warn("Transaction {} already billed, event {} recorded as processed", event.transactionId, eventId)
            return BillingOutcome.AlreadyProcessed
        }
        return BillingOutcome.Billed
    }
}
