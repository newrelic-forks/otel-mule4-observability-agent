package org.mule.extension.otel.mule4.observablity.agent.internal.metric;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages OpenTelemetry Counter metrics for tracking Mule flow and message processor traffic.
 */
public class MuleMetricTraffic {

    private static final Logger logger = LoggerFactory.getLogger(MuleMetricTraffic.class);
    private static MuleMetricTraffic muleMetricTraffic;
    private LongCounter flowTrafficCounter;
    private LongCounter processorTrafficCounter;

    /**
     * Private constructor to enforce singleton pattern.
     * Initializes the OpenTelemetry Meter and defines the traffic counters.
     * @param openTelemetry The OpenTelemetry instance for metric creation.
     */
    private MuleMetricTraffic(OpenTelemetry openTelemetry) {
        logger.info("Initializing the Traffic Metrics");
        Meter meter = openTelemetry.getMeter("org.mulesoft.extension.otel.mule4.observability.agent.metrics");

        flowTrafficCounter = meter.counterBuilder("mule.flow.traffic.total")
                .setDescription("Total number of Mule flow executions.")
                .setUnit("1")
                .build();

        processorTrafficCounter = meter.counterBuilder("mule.processor.traffic.total")
                .setDescription("Total number of Mule message processor invocations.")
                .setUnit("1")
                .build();
    }

    /**
     * Sets the singleton instance of MuleMetricTraffic. This method should be called once during SDK initialization.
     * @param ot The OpenTelemetry instance.
     */
    public static void setInstance(OpenTelemetry ot) {
        if (muleMetricTraffic == null) {
            muleMetricTraffic = new MuleMetricTraffic(ot);
        }
    }

    /**
     * Returns the singleton instance of MuleMetricTraffic.
     * @return The MuleMetricTraffic instance.
     */
    public static MuleMetricTraffic getInstance() {
        if (muleMetricTraffic == null) {
            // This should ideally not happen if setInstance is called during SDK initialization.
            // Log an error or throw an exception to indicate improper initialization.
            logger.error("MuleMetricTraffic not initialized. Call setInstance() first.");
            throw new IllegalStateException("MuleMetricTraffic not initialized.");
        }
        return muleMetricTraffic;
    }

    /**
     * Increments the counter for Mule flow executions.
     */
    public void recordFlowStart() {
        if (flowTrafficCounter != null) {
            flowTrafficCounter.add(1);
        }
    }

    /**
     * Increments the counter for Mule message processor invocations.
     */
    public void recordProcessorStart() {
        if (processorTrafficCounter != null) {
            processorTrafficCounter.add(1);
        }
    }
}