package org.mule.extension.otel.mule4.observablity.agent.internal.metric;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages OpenTelemetry Counter metrics for tracking Mule flow and message processor errors.
 */
public class MuleMetricErrors {

    private static final Logger logger = LoggerFactory.getLogger(MuleMetricErrors.class);
    private static MuleMetricErrors muleMetricErrors;
    private LongCounter flowErrorCounter;
    private LongCounter processorErrorCounter;

    /**
     * Private constructor to enforce singleton pattern.
     * Initializes the OpenTelemetry Meter and defines the error counters.
     * @param openTelemetry The OpenTelemetry instance for metric creation.
     */
    private MuleMetricErrors(OpenTelemetry openTelemetry) {
        Meter meter = openTelemetry.getMeter("org.mulesoft.extension.otel.mule4.observability.agent.metrics");

        // Mandatory: Error rate
        flowErrorCounter = meter.counterBuilder("mule.flow.errors.total")
                .setDescription("Total number of errors in Mule flow executions.")
                .setUnit("1")
                .build();

        processorErrorCounter = meter.counterBuilder("mule.processor.errors.total")
                .setDescription("Total number of errors in Mule message processor invocations.")
                .setUnit("1")
                .build();
    }

    /**
     * Sets the singleton instance of MuleMetricErrors. This method should be called once during SDK initialization.
     * @param ot The OpenTelemetry instance.
     */
    public static void setInstance(OpenTelemetry ot) {
        if (muleMetricErrors == null) {
            muleMetricErrors = new MuleMetricErrors(ot);
        }
    }

    /**
     * Returns the singleton instance of MuleMetricErrors.
     * @return The MuleMetricErrors instance.
     */
    public static MuleMetricErrors getInstance() {
        if (muleMetricErrors == null) {
            // This should ideally not happen if setInstance is called during SDK initialization.
            logger.error("MuleMetricErrors not initialized. Call setInstance() first.");
            throw new IllegalStateException("MuleMetricErrors not initialized.");
        }
        return muleMetricErrors;
    }

    /**
     * Increments the counter for errors in Mule flow executions.
     */
    public void recordFlowError() {
        if (flowErrorCounter != null) {
            flowErrorCounter.add(1);
        }
    }

    /**
     * Increments the counter for errors in Mule message processor invocations.
     */
    public void recordProcessorError() {
        if (processorErrorCounter != null) {
            processorErrorCounter.add(1);
        }
    }
}