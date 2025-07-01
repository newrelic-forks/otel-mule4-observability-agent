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

			// ------------------------------------------------------------------------------------
			// 	Copy over any WC3 Trace Headers from the incoming Http request into the current
			//	trace context
			// ------------------------------------------------------------------------------------
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
	private SpanBuilder addHttpListenerAttributesToSpan(EnrichedServerNotification notification,
			SpanBuilder spanBuilder) {
		HttpRequestAttributes httpRequestAttributes = NotificationParserUtils.getMessageAttributes(notification);

		try {
			MultiMap<String, String> requestHeaders = httpRequestAttributes.getHeaders();

			spanBuilder.setAttribute("scheme", httpRequestAttributes.getScheme());
			spanBuilder.setAttribute("http.method", httpRequestAttributes.getMethod()); 
			spanBuilder.setAttribute("remote.address", httpRequestAttributes.getRemoteAddress());
			spanBuilder.setAttribute("request.path", httpRequestAttributes.getRequestPath());
			   // ADDED: Setting http.target for better transaction naming in APM tools
            // This is a standard OpenTelemetry semantic convention for the full request target
            String requestPath = httpRequestAttributes.getRequestPath();
            String queryString = httpRequestAttributes.getQueryString();
            String httpTarget = requestPath;
            if (queryString != null && !queryString.isEmpty()) {
                httpTarget += "?" + queryString;
            }
            spanBuilder.setAttribute("http.target", httpTarget);
         // OPTIONAL: If you have configured routing (e.g., using a base path/route)
             spanBuilder.setAttribute("http.route", httpRequestAttributes.getListenerPath()); 

			// Obtain excluded headers
			Set<String> excludedHeadersSet = new HashSet<>();
			OtelSdkConnection otelSdkConnection = OtelSdkConnection.get().orElse(null);
			 if (otelSdkConnection != null) {
		            CustomAttributesConfig customAttributesConfig = otelSdkConnection.getCustomAttributesConfig().orElse(null);
		            if (customAttributesConfig != null) {
		                String excludedHeaders = customAttributesConfig.getExcludedHeaders();
		                if (excludedHeaders != null) {
		                    excludedHeadersSet = Arrays.stream(excludedHeaders.split(","))
		                                               .map(String::trim) // Trim spaces for each header
		                                               .collect(Collectors.toSet());
		                }
		            }
		        }

			// Use lambda expression to apply filtering logic
	     final Set<String> headersToExclude = excludedHeadersSet; // Make effectively final by using a separate final reference
	        requestHeaders.forEach((key, collection) -> {
	            if (!headersToExclude.contains(key)) {
	            	  spanBuilder.setAttribute("headers." + key, collection);
	            } else {
	                logger.debug("Excluding header from span: " + key);
	            }
	        });

		} catch (Exception e) {
			logger.debug(e.getMessage());
		}

		return spanBuilder;
	}

}
