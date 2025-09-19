package org.mule.extension.otel.mule4.observablity.agent.internal.metric;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages OpenTelemetry Histogram metrics for tracking HTTP client and server metrics.
 */
public class MuleMetricHttp {
    private static final Logger logger = LoggerFactory.getLogger(MuleMetricHttp.class);
    private static MuleMetricHttp muleMetricHttp;

    private DoubleHistogram httpClientRequestDuration;
    private DoubleHistogram httpServerRequestDuration;
    private DoubleHistogram httpServerRequestBodySize;
    private DoubleHistogram httpServerResponseBodySize;
    private DoubleHistogram httpClientRequestBodySize;
    private DoubleHistogram httpClientResponseBodySize;

    /**
     * Private constructor to enforce singleton pattern.
     * Initializes the OpenTelemetry Meter and defines the HTTP metrics.
     *
     * @param openTelemetry The OpenTelemetry instance for metric creation.
     */
    private MuleMetricHttp(OpenTelemetry openTelemetry) {
        logger.info("Initializing the HTTP Metrics");
        Meter meter = openTelemetry.getMeter("org.mulesoft.extension.otel.mule4.observability.agent.metrics");

        // HTTP Client Request Duration
        httpClientRequestDuration = meter.histogramBuilder("http.client.request.duration")
                .setDescription("Duration of HTTP client requests.")
                .setUnit("ms")
                .build();

        // HTTP Server Request Duration
        httpServerRequestDuration = meter.histogramBuilder("http.server.request.duration")
                .setDescription("Duration of HTTP server requests.")
                .setUnit("ms")
                .build();

        // HTTP Server Request Body Size
        httpServerRequestBodySize = meter.histogramBuilder("http.server.request.body.size")
                .setDescription("Size of HTTP server request bodies.")
                .setUnit("bytes")
                .build();

        // HTTP Server Response Body Size
        httpServerResponseBodySize = meter.histogramBuilder("http.server.response.body.size")
                .setDescription("Size of HTTP server response bodies.")
                .setUnit("bytes")
                .build();

        // HTTP Client Request Body Size
        httpClientRequestBodySize = meter.histogramBuilder("http.client.request.body.size")
                .setDescription("Size of HTTP client request bodies.")
                .setUnit("bytes")
                .build();

        // HTTP Client Response Body Size
        httpClientResponseBodySize = meter.histogramBuilder("http.client.response.body.size")
                .setDescription("Size of HTTP client response bodies.")
                .setUnit("bytes")
                .build();
    }

    /**
     * Sets the singleton instance of MuleMetricHttp. This method should be called once during SDK initialization.
     *
     * @param ot The OpenTelemetry instance.
     */
    public static void setInstance(OpenTelemetry ot) {
        if (muleMetricHttp == null) {
            muleMetricHttp = new MuleMetricHttp(ot);
        }
    }

    /**
     * Records the duration of an HTTP client request.
     *
     * @param durationMs The duration of the request in milliseconds.
     * @param attributes The attributes associated with the request.
     */
    public void recordHttpClientRequest(double durationMs, Attributes attributes) {
        if (httpClientRequestDuration != null) {
            logger.debug("Recording HTTP client request duration: {} ms with attributes: {}", durationMs, attributes);
            httpClientRequestDuration.record(durationMs, attributes);
        }
    }

    /**
     * Records the duration of an HTTP server request.
     *
     * @param durationMs The duration of the request in milliseconds.
     * @param attributes The attributes associated with the request.
     */
    public void recordHttpServerRequest(double durationMs, Attributes attributes) {
        if (httpServerRequestDuration != null) {
            logger.info("Recording HTTP server request duration: {} ms with attributes: {}", durationMs, attributes);
            httpServerRequestDuration.record(durationMs, attributes);
        }
    }

    /**
     * Records the size of an HTTP server request body.
     *
     * @param size The size of the request body in bytes.
     * @param attributes The attributes associated with the request.
     */
    public void recordHttpServerRequestBodySize(long size, Attributes attributes) {
        if (httpServerRequestBodySize != null) {
            logger.debug("Recording HTTP server request body size: {} bytes with attributes: {}", size, attributes);
            httpServerRequestBodySize.record(size, attributes);
        }
    }

    /**
     * Records the size of an HTTP server response body.
     *
     * @param size The size of the response body in bytes.
     * @param attributes The attributes associated with the response.
     */
    public void recordHttpServerResponseBodySize(long size, Attributes attributes) {
        if (httpServerResponseBodySize != null) {
            logger.debug("Recording HTTP server response body size: {} bytes with attributes: {}", size, attributes);
            httpServerResponseBodySize.record(size, attributes);
        }
    }

    /**
     * Records the size of an HTTP client request body.
     *
     * @param size The size of the request body in bytes.
     * @param attributes The attributes associated with the request.
     */
    public void recordHttpClientRequestBodySize(long size, Attributes attributes) {
        if (httpClientRequestBodySize != null) {
            logger.debug("Recording HTTP client request body size: {} bytes with attributes: {}", size, attributes);
            httpClientRequestBodySize.record(size, attributes);
        }
    }

    /**
     * Records the size of an HTTP client response body.
     *
     * @param size The size of the response body in bytes.
     * @param attributes The attributes associated with the response.
     */
    public void recordHttpClientResponseBodySize(long size, Attributes attributes) {
        if (httpClientResponseBodySize != null) {
            logger.debug("Recording HTTP client response body size: {} bytes with attributes: {}", size, attributes);
            httpClientResponseBodySize.record(size, attributes);
        }
    }

    /**
     * Returns the singleton instance of MuleMetricHttp.
     *
     * @return The MuleMetricHttp instance.
     * @throws IllegalStateException if the instance has not been initialized via {@code setInstance()}.
     */
    public static MuleMetricHttp getInstance() {
        if (muleMetricHttp == null) {
            logger.error("MuleMetricHttp not initialized. Call setInstance() first.");
            throw new IllegalStateException("MuleMetricHttp not initialized.");
        }
        return muleMetricHttp;
    }
}
