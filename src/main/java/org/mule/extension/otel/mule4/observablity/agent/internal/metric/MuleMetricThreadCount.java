package org.mule.extension.otel.mule4.observablity.agent.internal.metric;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;

/**
 * Reports JVM thread count. High thread count can indicate thread leaks
 * or uncontrolled parallelism that leads to CPU and memory exhaustion.
 */
public class MuleMetricThreadCount
{
    private static final Logger logger = LoggerFactory.getLogger(MuleMetricThreadCount.class);

    private static MuleMetricThreadCount instance;
    private static ThreadMXBean threadMxBean;

    private MuleMetricThreadCount(OpenTelemetry openTelemetry)
    {
        logger.info("Initializing the JVM Thread Count Metric");

        Meter meter = openTelemetry.getMeter("org.mulesoft.extension.otel.mule4.observability.agent.metrics");

        Consumer<ObservableDoubleMeasurement> recordMeasure = (result) -> MuleMetricThreadCount.record(result);

        threadMxBean = ManagementFactory.getThreadMXBean();

        meter.gaugeBuilder("jvm.thread.count")
             .setDescription("Reports current JVM thread count.")
             .setUnit("threads")
             .buildWithCallback(recordMeasure);
    }

    public static void record(ObservableDoubleMeasurement measure)
    {
        if (threadMxBean != null)
        {
            Attributes daemonAttr = Attributes.of(AttributeKey.booleanKey("jvm.thread.daemon"), true);
            Attributes nonDaemonAttr = Attributes.of(AttributeKey.booleanKey("jvm.thread.daemon"), false);

            int total = threadMxBean.getThreadCount();
            int daemon = threadMxBean.getDaemonThreadCount();

            measure.record(daemon, daemonAttr);
            measure.record(total - daemon, nonDaemonAttr);
        }
    }

    public static void setInstance(OpenTelemetry ot)
    {
        if (instance == null)
        {
            instance = new MuleMetricThreadCount(ot);
        }
    }
}
