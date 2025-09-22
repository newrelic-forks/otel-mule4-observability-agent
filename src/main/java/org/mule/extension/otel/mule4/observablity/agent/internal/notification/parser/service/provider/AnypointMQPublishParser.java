package org.mule.extension.otel.mule4.observablity.agent.internal.notification.parser.service.provider;

import java.util.Map;

import org.mule.extension.otel.mule4.observablity.agent.internal.context.propagation.OTelContextPropagator;
import org.mule.extension.otel.mule4.observablity.agent.internal.context.propagation.SimpleHashMapSetter;
import org.mule.extension.otel.mule4.observablity.agent.internal.store.config.MuleConnectorConfigStore;
import org.mule.extension.otel.mule4.observablity.agent.internal.store.trace.MuleSoftTraceStore;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.Constants;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.NotificationParserUtils;
import org.mule.runtime.api.notification.EnrichedServerNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;

import com.mulesoft.extension.mq.api.attributes.AnypointMQMessagePublishAttributes;

public class AnypointMQPublishParser extends BaseNotificationParser
{
    private static Logger logger = LoggerFactory.getLogger(AnypointMQPublishParser.class);

    // --------------------------------------------------------------------------------------------
    // Verify if this Parser can handle this notification
    // -------------------------------------------------------------------------------------------- 
    @Override
    public boolean canParse(EnrichedServerNotification notification)
    {
        if (NotificationParserUtils.getComponentId(notification).equalsIgnoreCase(Constants.ANYPOINT_MQ_PUBLISH))
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
        
        return addMQPublishAttributesToSpan(notification, muleConnectorConfigStore, spanBuilder);
    }

    // --------------------------------------------------------------------------------------------
    // Annotate the span with various MQ Publish attributes
    // --------------------------------------------------------------------------------------------
    private SpanBuilder addMQPublishAttributesToSpan(EnrichedServerNotification notification,
                                                     MuleConnectorConfigStore muleConnectorConfigStore,
                                                     SpanBuilder spanBuilder)
    {
        Map<String, String> anypointMQAttributes = NotificationParserUtils.getComponentAnnotation("{config}componentParameters", notification);
        String configRef = anypointMQAttributes.get("config-ref");

        MuleConnectorConfigStore.AnypointMQConfig anypointMQConfig = muleConnectorConfigStore.getConfig(configRef);

        try 
        {
            spanBuilder.setAttribute("publish.scheme", anypointMQConfig.getProtocol());
            spanBuilder.setAttribute("publish.host", anypointMQConfig.getHost());
            spanBuilder.setAttribute("publish.port", anypointMQConfig.getPort());
            spanBuilder.setAttribute("publish.path", anypointMQConfig.getPath());            
            spanBuilder.setAttribute("publish.clientId", anypointMQConfig.getClientId());            
          
            spanBuilder.setAttribute("messaging.system", "anypointmq");
            spanBuilder.setAttribute("messaging.destination", anypointMQAttributes.get("destination"));
            spanBuilder.setAttribute("messaging.destination_kind", "queue"); // or "topic"
            spanBuilder.setAttribute("messaging.operation", "send");
            spanBuilder.setAttribute("net.peer.name", anypointMQConfig.getHost());
            spanBuilder.setAttribute("net.peer.port", anypointMQConfig.getPort());
            spanBuilder.setAttribute("messaging.url", anypointMQConfig.getPath());
            spanBuilder.setAttribute("messaging.client_id", anypointMQConfig.getClientId());        
            // Inject context into outgoing HTTP headers
         // Inject context into outgoing MQ message properties
        Map<String, String> userProperties = anypointMQConfig.getUserProperties();
        if (userProperties == null) {
            userProperties = new java.util.HashMap<>();
            anypointMQConfig.setUserProperties(userProperties); // --- FIX: Set back to config
        }
        OTelContextPropagator.inject(userProperties, new SimpleHashMapSetter());
        }
        catch (Exception e)
        {
            logger.debug(e.getMessage());
        }

        return spanBuilder;
    }
    
   
    // --------------------------------------------------------------------------------------------
    // Message Processor End Notification Parsing Handler
    // -------------------------------------------------------------------------------------------- 
    @Override
    public void endProcessorNotification(EnrichedServerNotification notification, MuleSoftTraceStore traceStore)
    {
        super.endProcessorNotification(notification, traceStore);
        addMQPublishResponseAttributesToSpan(notification, traceStore);

    }
    
    // --------------------------------------------------------------------------------------------
    // Annotate the span with various MQ Publish Response attributes
    // --------------------------------------------------------------------------------------------

    private void addMQPublishResponseAttributesToSpan(EnrichedServerNotification notification, MuleSoftTraceStore traceStore)
    {
        AnypointMQMessagePublishAttributes mqPublishAttributes = NotificationParserUtils.getMessageAttributes(notification);
        Map<String, String> userProperties = new java.util.HashMap<>();
        try {
            OTelContextPropagator.inject(userProperties, new SimpleHashMapSetter());
        } catch (Exception e) {
            logger.debug(e.getMessage());
        }

        Span span = traceStore.getMessageProcessorSpan(NotificationParserUtils.getMuleSoftTraceId(notification), 
                                                       NotificationParserUtils.getFlowId(notification), 
                                                       NotificationParserUtils.getSpanId(notification));
        try
        {    
            span.setAttribute("messaging.message_id", mqPublishAttributes.getMessageId());
        }
        catch (Exception e)
        {
            logger.debug(e.getMessage());
        }
    }
   
}
