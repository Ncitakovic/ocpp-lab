package dev.ocpplab.billing

import dev.ocpplab.events.SessionClosed
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

    /**
     * Bills one closed session. processed_event and billing_record are written in ONE transaction:
     * both rows commit, or neither.
     */
    @Transactional
    fun bill(eventId: UUID, event: SessionClosed): BillingOutcome {
        val now = clock.instant()

        // TODO (b) [user]: idempotency check on processed_event, in this same transaction.
        //
        //  - If eventId was already processed: write nothing and return BillingOutcome.AlreadyProcessed.
        //    The listener still acknowledges the offset, so the redelivered message is skipped.
        //  - Otherwise: record eventId in processed_event (processed_at = now) and fall through
        //    to the billing_record insert below.
        //
        //  Available: processedEvents.exists(eventId) and processedEvents.insertIfAbsent(eventId, now)
        //  (returns 1 if inserted, 0 if already there). Think about two deliveries of the same
        //  event running at the same moment: which version leaves no gap between "check" and "insert"?
        //
        //  Test that checks this: BillingIntegrationTest `same event billed twice ...`.

        val energyWh = event.meterStop.toLong() - event.meterStart
        billingRecords.insert(
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
        return BillingOutcome.Billed
    }

    /** Records eventId as processed without billing (used when the session was already billed). */
    @Transactional
    fun markProcessed(eventId: UUID): Boolean = processedEvents.insertIfAbsent(eventId, clock.instant()) == 1
}
