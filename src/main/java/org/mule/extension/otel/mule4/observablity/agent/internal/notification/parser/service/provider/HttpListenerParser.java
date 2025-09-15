package org.mule.extension.otel.mule4.observablity.agent.internal.notification.parser.service.provider;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.mule.extension.http.api.HttpRequestAttributes;
import org.mule.extension.otel.mule4.observablity.agent.internal.config.advanced.CustomAttributesConfig;
import org.mule.extension.otel.mule4.observablity.agent.internal.connection.OtelSdkConnection;
import org.mule.extension.otel.mule4.observablity.agent.internal.context.propagation.HttpRequestAttributesGetter;
import org.mule.extension.otel.mule4.observablity.agent.internal.context.propagation.OTelContextPropagator;
import org.mule.extension.otel.mule4.observablity.agent.internal.store.config.MuleConnectorConfigStore;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.Constants;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.NotificationParserUtils;
import org.mule.runtime.api.component.ComponentIdentifier;
import org.mule.runtime.api.notification.EnrichedServerNotification;
import org.mule.runtime.api.notification.PipelineMessageNotification;
import org.mule.runtime.api.util.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;

public class HttpListenerParser extends BaseNotificationParser
{
	private static Logger logger = LoggerFactory.getLogger(HttpListenerParser.class);

	// --------------------------------------------------------------------------------------------
	// Verify if this Parser can handle this notification
	// --------------------------------------------------------------------------------------------	
	@Override
	public boolean canParse(EnrichedServerNotification notification)
	{
		ComponentIdentifier sourceIdentifier = NotificationParserUtils.getSourceIdentifier(notification);
		String sourceComponent = sourceIdentifier.getNamespace() + ":" + sourceIdentifier.getName();
		int action = Integer.parseInt(notification.getAction().getIdentifier());

		// ----------------------------------------------------------------------------------------
		// Only parse HTTP Listener notifications which are a source/trigger to the start of a flow
		// ----------------------------------------------------------------------------------------
		if ( sourceComponent.equalsIgnoreCase(Constants.HTTP_LISTENER)  && 
				action == PipelineMessageNotification.PROCESS_START )
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Pipeline Start Notification Parsing Handler
	// --------------------------------------------------------------------------------------------
	@Override
	public SpanBuilder startPipelineNotification(EnrichedServerNotification notification,
			MuleConnectorConfigStore muleConnectorConfigStore, 
			SpanBuilder spanBuilder)
	{
		super.startPipelineNotification(notification, muleConnectorConfigStore, spanBuilder);

		try
		{
			spanBuilder.setSpanKind(SpanKind.SERVER);
			spanBuilder = addHttpListenerAttributesToSpan(notification, spanBuilder);

			HttpRequestAttributes httpRequestAttributes = NotificationParserUtils.getMessageAttributes(notification);

			// Extract context from incoming HTTP request
			Context context = OTelContextPropagator.extract(httpRequestAttributes, new HttpRequestAttributesGetter());
			spanBuilder.setParent(context);	
		}
		catch (Exception e)
		{
			logger.debug(e.getMessage());
		}

		return spanBuilder;
	}
	

	// --------------------------------------------------------------------------------------------
	// Annotate the span with various HTTP Listener attributes
	// --------------------------------------------------------------------------------------------
	private SpanBuilder addHttpListenerAttributesToSpan(EnrichedServerNotification notification, SpanBuilder spanBuilder) {
    HttpRequestAttributes httpRequestAttributes = NotificationParserUtils.getMessageAttributes(notification);

    // Mandatory OTEL attributes for HTTP server spans/metrics
    spanBuilder.setAttribute("http.request.method", httpRequestAttributes.getMethod()); // mandatory
    spanBuilder.setAttribute("url.scheme", httpRequestAttributes.getScheme()); // mandatory
	// Try to get status code from headers if available
	String statusCode = null;
	if (httpRequestAttributes.getHeaders() != null && httpRequestAttributes.getHeaders().containsKey("status")) {
		statusCode = httpRequestAttributes.getHeaders().get("status");
	}
	spanBuilder.setAttribute("http.response.status_code", statusCode); // mandatory if available
	spanBuilder.setAttribute("http.route", httpRequestAttributes.getListenerPath()); // mandatory if available

	// Optional attributes if available
	spanBuilder.setAttribute("url.path", httpRequestAttributes.getRequestPath()); // optional
	spanBuilder.setAttribute("url.query", httpRequestAttributes.getQueryString()); // optional
	spanBuilder.setAttribute("server.address", httpRequestAttributes.getLocalAddress()); // optional

	// Try to get local port from headers if available
	String localPort = null;
	if (httpRequestAttributes.getHeaders() != null && httpRequestAttributes.getHeaders().containsKey("localPort")) {
		localPort = httpRequestAttributes.getHeaders().get("localPort");
	}
	spanBuilder.setAttribute("server.port", localPort); // optional

	spanBuilder.setAttribute("network.protocol.name", httpRequestAttributes.getScheme() != null ? httpRequestAttributes.getScheme() : "http"); // optional, can be derived

	// Try to get protocol version from headers if available
	String protocolVersion = null;
	if (httpRequestAttributes.getHeaders() != null && httpRequestAttributes.getHeaders().containsKey("protocolVersion")) {
		protocolVersion = httpRequestAttributes.getHeaders().get("protocolVersion");
	}
	spanBuilder.setAttribute("network.protocol.version", protocolVersion); // optional

	spanBuilder.setAttribute("client.address", httpRequestAttributes.getRemoteAddress()); // optional

	// Try to get User-Agent from headers if available
	String userAgent = null;
	if (httpRequestAttributes.getHeaders() != null && httpRequestAttributes.getHeaders().containsKey("User-Agent")) {
		userAgent = httpRequestAttributes.getHeaders().get("User-Agent");
	}
	spanBuilder.setAttribute("user_agent.original", userAgent); // optional

	// Error handling: Try to get error type from headers if available
	String errorType = null;
	if (httpRequestAttributes.getHeaders() != null && httpRequestAttributes.getHeaders().containsKey("errorType")) {
		errorType = httpRequestAttributes.getHeaders().get("errorType");
	}
	if (errorType != null) {
		spanBuilder.setAttribute("error.type", errorType); // mandatory if error
	}

    // Headers (opt-in, explicit config)
    MultiMap<String, String> requestHeaders = httpRequestAttributes.getHeaders();
    Set<String> excludedHeadersSet = new HashSet<String>();
    OtelSdkConnection otelSdkConnection = OtelSdkConnection.get().orElse(null);
    if (otelSdkConnection != null) {
        CustomAttributesConfig customAttributesConfig = otelSdkConnection.getCustomAttributesConfig().orElse(null);
        if (customAttributesConfig != null) {
            String excludedHeaders = customAttributesConfig.getExcludedHeaders();
            if (excludedHeaders != null) {
                String[] split = excludedHeaders.split(",");
                for (String header : split) {
                    excludedHeadersSet.add(header.trim());
                }
            }
        }
    }
    for (String key : requestHeaders.keySet()) {
        if (!excludedHeadersSet.contains(key)) {
            spanBuilder.setAttribute("http.request.header." + key.toLowerCase(), requestHeaders.getAll(key).toString()); // optional
        }
    }

    return spanBuilder;
}
}
