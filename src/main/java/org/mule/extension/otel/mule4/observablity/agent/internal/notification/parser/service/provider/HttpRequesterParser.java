package org.mule.extension.otel.mule4.observablity.agent.internal.notification.parser.service.provider;

import java.util.Map;

import org.mule.extension.http.api.HttpResponseAttributes;
import org.mule.extension.otel.mule4.observablity.agent.internal.context.propagation.OTelContextPropagator;
import org.mule.extension.otel.mule4.observablity.agent.internal.context.propagation.SimpleHashMapSetter;
import org.mule.extension.otel.mule4.observablity.agent.internal.store.config.MuleConnectorConfigStore;
import org.mule.extension.otel.mule4.observablity.agent.internal.store.trace.MuleSoftTraceStore;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.Constants;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.NotificationParserUtils;
import org.mule.runtime.api.notification.EnrichedServerNotification;
import org.mule.runtime.api.util.MultiMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;

public class HttpRequesterParser extends BaseNotificationParser
{
	private static Logger logger = LoggerFactory.getLogger(HttpRequesterParser.class);

	// --------------------------------------------------------------------------------------------
	// Verify if this Parser can handle this notification
	// --------------------------------------------------------------------------------------------	
	@Override
	public boolean canParse(EnrichedServerNotification notification)
	{
		if (NotificationParserUtils.getComponentId(notification).equalsIgnoreCase(Constants.HTTP_REQUESTER))
			return true;
		else
			return false;
	}
	
	// --------------------------------------------------------------------------------------------
	// Message Processor Start Notification Parsing Handler
	// --------------------------------------------------------------------------------------------	
	@Override
	public SpanBuilder startProcessorNotification(EnrichedServerNotification notification,
			                                      MuleConnectorConfigStore muleConnectorConfigStore, 
			                                      SpanBuilder spanBuilder)
	{
		super.startProcessorNotification(notification, muleConnectorConfigStore, spanBuilder);
		
		spanBuilder.setSpanKind(SpanKind.CLIENT);
		
		return addHttpRequesterAttributesToSpan(notification, muleConnectorConfigStore, spanBuilder);
	}

	// --------------------------------------------------------------------------------------------
	// Message Processor End Notification Parsing Handler
	// --------------------------------------------------------------------------------------------	
	@Override
	public void endProcessorNotification(EnrichedServerNotification notification, MuleSoftTraceStore traceStore)
	{
		super.endProcessorNotification(notification, traceStore);
		addHttpResponseAttributesToSpan(notification, traceStore);
	}

	// --------------------------------------------------------------------------------------------
	// Annotate the span with various HTTP Requester attributes
	// --------------------------------------------------------------------------------------------
	private SpanBuilder addHttpRequesterAttributesToSpan(EnrichedServerNotification notification,
                                                     MuleConnectorConfigStore muleConnectorConfigStore,
                                                     SpanBuilder spanBuilder)
	{
		Map<String, String> requesterAttributes = NotificationParserUtils.getComponentAnnotation("{config}componentParameters", notification);
		String configRef = requesterAttributes.get("config-ref");
		MuleConnectorConfigStore.HttpRequesterConfig httpRequesterConfig = muleConnectorConfigStore.getConfig(configRef);

		// Mandatory OTEL attributes for HTTP client spans/metrics
		spanBuilder.setAttribute("http.request.method", requesterAttributes.get("method")); // mandatory
		spanBuilder.setAttribute("server.address", httpRequesterConfig.getHost()); // mandatory
		spanBuilder.setAttribute("server.port", httpRequesterConfig.getPort()); // mandatory
		spanBuilder.setAttribute("url.full", requesterAttributes.get("url")); // mandatory
		spanBuilder.setAttribute("http.response.status_code", requesterAttributes.get("statusCode")); // mandatory if available

		// Optional attributes if available
		spanBuilder.setAttribute("network.protocol.name", "http"); // optional
		spanBuilder.setAttribute("network.protocol.version", requesterAttributes.get("protocolVersion")); // optional
		spanBuilder.setAttribute("url.scheme", httpRequesterConfig.getProtocol()); // optional
		spanBuilder.setAttribute("url.template", requesterAttributes.get("pathTemplate")); // optional
		spanBuilder.setAttribute("user_agent.original", requesterAttributes.get("userAgent")); // optional

		// Error handling
		if (requesterAttributes.get("errorType") != null) {
			spanBuilder.setAttribute("error.type", requesterAttributes.get("errorType")); // mandatory if error
		}

		// Headers (opt-in, explicit config)
		Map<String, java.util.List<String>> headersMap = httpRequesterConfig.getHeaders();
		MultiMap<String, String> requestHeaders = new MultiMap<String, String>();
		if (headersMap != null) {
			for (Map.Entry<String, java.util.List<String>> entry : headersMap.entrySet()) {
				String key = entry.getKey();
				java.util.List<String> valueList = entry.getValue();
				if (valueList != null) {
					for (String value : valueList) {
						requestHeaders.put(key, value);
					}
				}
			}
		}
		for (String key : requestHeaders.keySet()) {
			spanBuilder.setAttribute("http.request.header." + key.toLowerCase(), requestHeaders.getAll(key).toString()); // optional
		}

		return spanBuilder;
	}
	
	// --------------------------------------------------------------------------------------------
	// Annotate the span with various HTTP Response attributes
	// --------------------------------------------------------------------------------------------

	private void addHttpResponseAttributesToSpan(EnrichedServerNotification notification, MuleSoftTraceStore traceStore)
	{
	    HttpResponseAttributes responseAttributes = NotificationParserUtils.getMessageAttributes(notification);

	    Span span = traceStore.getMessageProcessorSpan(NotificationParserUtils.getMuleSoftTraceId(notification), 
	                                                   NotificationParserUtils.getFlowId(notification), 
	                                                   NotificationParserUtils.getSpanId(notification));
	    try
	    {
	        MultiMap<String, String> responseHeaders = responseAttributes.getHeaders();

	        // --- Mandatory ---
	        span.setAttribute("http.response.status_code", responseAttributes.getStatusCode()); // REQUIRED

	        // --- Recommended ---
	        span.setAttribute("response.reason.phrase", responseAttributes.getReasonPhrase()); // recommended

	        // --- Headers (opt-in, explicit config) ---
	        for (String key : responseHeaders.keySet()) {
	            span.setAttribute("http.response.header." + key.toLowerCase(), responseHeaders.getAll(key).toString()); // opt-in
	        }
	    }
	    catch (Exception e)
	    {
	        logger.debug(e.getMessage());
	    }
	}

}
