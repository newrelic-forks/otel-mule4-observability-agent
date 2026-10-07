package org.mule.extension.otel.mule4.observablity.agent.internal.metric;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;

/**
 * Reports JVM Garbage Collection cumulative duration in milliseconds.
 * High GC duration directly explains CPU spikes — when GC is thrashing,
 * it consumes CPU cycles and causes application pauses.
 */
public class MuleMetricGcDuration
{
    private static final Logger logger = LoggerFactory.getLogger(MuleMetricGcDuration.class);

    private static MuleMetricGcDuration instance;
    private static List<GarbageCollectorMXBean> gcBeans;

    private MuleMetricGcDuration(OpenTelemetry openTelemetry)
    {
        logger.info("Initializing the JVM GC Duration Metric");

        Meter meter = openTelemetry.getMeter("org.mulesoft.extension.otel.mule4.observability.agent.metrics");

        Consumer<ObservableDoubleMeasurement> recordMeasure = (result) -> MuleMetricGcDuration.record(result);

        gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

        meter.gaugeBuilder("jvm.gc.collection_time")
             .setDescription("Reports cumulative JVM garbage collection time in milliseconds per collector.")
             .setUnit("ms")
             .buildWithCallback(recordMeasure);
    }

    public static void record(ObservableDoubleMeasurement measure)
    {
        if (gcBeans != null)
        {
            for (GarbageCollectorMXBean gcBean : gcBeans)
            {
                Attributes attribute = Attributes.of(
                    AttributeKey.stringKey("jvm.gc.name"), gcBean.getName()
                );
                long gcTime = gcBean.getCollectionTime();
                if (gcTime >= 0)
                {
                    measure.record(gcTime, attribute);
                }
            }
        }
    }

    public static void setInstance(OpenTelemetry ot)
    {
        if (instance == null)
        {
            instance = new MuleMetricGcDuration(ot);
        }
    }
}
