
package org.mule.extension.otel.mule4.observablity.agent.internal.metric;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Manages OpenTelemetry Histogram metrics for tracking Mule flow and message processor latency.
 * Latency is a crucial Golden Signal, representing the time taken to complete a request or operation.
 */
public class MuleMetricLatency {

    private static final Logger logger = LoggerFactory.getLogger(MuleMetricLatency.class);
    private static MuleMetricLatency muleMetricLatency;
    private DoubleHistogram flowLatencyHistogram;
    private DoubleHistogram processorLatencyHistogram;

    /**
     * Private constructor to enforce singleton pattern.
     * Initializes the OpenTelemetry Meter and defines the latency histograms.
     * Histograms are used to record distributions of values, like latency, and can
     * provide aggregated data such as sum, count, min, max, and percentile approximations.
     *
     * @param openTelemetry The OpenTelemetry instance for metric creation.
     */
    private MuleMetricLatency(OpenTelemetry openTelemetry) {
        logger.info("Initializing the Latency Metrics");
        Meter meter = openTelemetry.getMeter("org.mulesoft.extension.otel.mule4.observability.agent.metrics");

        // Histogram for overall flow latency
        flowLatencyHistogram = meter.histogramBuilder("mule.flow.latency")
                .setDescription("Measures the duration of Mule flow executions.")
                .setUnit("ms") // Milliseconds for latency
                .build();

        // Histogram for individual message processor latency
        processorLatencyHistogram = meter.histogramBuilder("mule.processor.latency")
                .setDescription("Measures the duration of Mule message processor invocations.")
                .setUnit("ms") // Milliseconds for latency
                .build();
    }

    /**
     * Sets the singleton instance of MuleMetricLatency. This method should be called once during SDK initialization.
     *
     * @param ot The OpenTelemetry instance.
     */
    public static void setInstance(OpenTelemetry ot) {
        if (muleMetricLatency == null) {
            muleMetricLatency = new MuleMetricLatency(ot);
        }
    }

    /**
     * Returns the singleton instance of MuleMetricLatency.
     *
     * @return The MuleMetricLatency instance.
     * @throws IllegalStateException if the instance has not been initialized via {@code setInstance()}.
     */
    public static MuleMetricLatency getInstance() {
        if (muleMetricLatency == null) {
            logger.error("MuleMetricLatency not initialized. Call setInstance() first.");
            throw new IllegalStateException("MuleMetricLatency not initialized.");
        }
        return muleMetricLatency;
    }

    /**
     * Records the latency for a Mule flow.
     *
     * @param durationMs The duration of the flow execution in milliseconds.
     * @param flowName   The name of the Mule flow.
     */
    public void recordFlowLatency(double durationMs, String flowName) {
        if (flowLatencyHistogram != null) {
            Attributes attributes = Attributes.of(AttributeKey.stringKey("flow.name"), flowName);
            // ADD THIS DEBUG LOG:
            logger.debug("Recording flow latency: {} ms for flow: {} with attributes: {}", durationMs, flowName, attributes);
            flowLatencyHistogram.record(durationMs, attributes);
            logger.debug("Recorded flow latency: {} ms for flow: {}", durationMs, flowName);
        }
    }

    /**
     * Records the latency for a Mule message processor.
     *
     * @param durationMs  The duration of the processor invocation in milliseconds.
     * @param componentId The qualified name of the component (e.g., "http:request", "db:select").
     * @param docName     The user-defined name of the message processor (doc:name).
     */
    public void recordProcessorLatency(double durationMs, String componentId, String docName) {
        if (processorLatencyHistogram != null) {
            Attributes attributes = Attributes.builder()
                    .put(AttributeKey.stringKey("component.id"), componentId)
                    .put(AttributeKey.stringKey("doc.name"), docName)
                    .build();
            // ADD THIS DEBUG LOG:
            logger.debug("Recording processor latency: {} ms for component: {} ({}) with attributes: {}", durationMs, componentId, docName, attributes);
            processorLatencyHistogram.record(durationMs, attributes);
            logger.debug("Recorded processor latency: {} ms for component: {} ({})", durationMs, componentId, docName);
        }
    }
}