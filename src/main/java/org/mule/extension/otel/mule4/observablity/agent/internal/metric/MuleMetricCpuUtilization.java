package org.mule.extension.otel.mule4.observablity.agent.internal.metric;

import java.lang.management.ManagementFactory;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;

/**
 * Reports JVM process CPU utilization as a percentage (0-100).
 * Uses com.sun.management.OperatingSystemMXBean.getCpuLoad() which returns
 * the "recent cpu usage" for the JVM process (0.0 to 1.0).
 * This matches what Anypoint Platform dashboard shows as CPU %.
 */
public class MuleMetricCpuUtilization
{
    private static final Logger logger = LoggerFactory.getLogger(MuleMetricCpuUtilization.class);

    private static MuleMetricCpuUtilization instance;
    private static com.sun.management.OperatingSystemMXBean osMxBean;

    private MuleMetricCpuUtilization(OpenTelemetry openTelemetry)
    {
        logger.info("Initializing the JVM CPU Utilization Metric");

        Meter meter = openTelemetry.getMeter("org.mulesoft.extension.otel.mule4.observability.agent.metrics");

        Consumer<ObservableDoubleMeasurement> recordMeasure = (result) -> MuleMetricCpuUtilization.record(result);

        try
        {
            osMxBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        }
        catch (ClassCastException e)
        {
            logger.warn("com.sun.management.OperatingSystemMXBean not available. JVM CPU metric will report 0.");
            osMxBean = null;
        }

        meter.gaugeBuilder("process.cpu.utilization")
             .setDescription("Reports JVM process CPU utilization as a ratio (0.0 to 1.0).")
             .setUnit("1")
             .buildWithCallback(recordMeasure);
    }

    public static void record(ObservableDoubleMeasurement measure)
    {
        Attributes attribute = Attributes.of(AttributeKey.stringKey("process.cpu.state"), "user");

        if (osMxBean != null)
        {
            measure.record(getCpuUtilization(), attribute);
        }
    }

    /**
     * Returns JVM process CPU utilization as a ratio (0.0 to 1.0).
     * OTel semantic convention: process.cpu.utilization uses unit "1" (ratio).
     */
    public static double getCpuUtilization()
    {
        if (osMxBean == null) return 0.0;

        double cpuLoad = osMxBean.getCpuLoad();
        // getCpuLoad() returns -1.0 if not available, otherwise 0.0 to 1.0
        if (cpuLoad < 0) return 0.0;
        return cpuLoad;
    }

    public static void setInstance(OpenTelemetry ot)
    {
        if (instance == null)
        {
            instance = new MuleMetricCpuUtilization(ot);
        }
    }
}
