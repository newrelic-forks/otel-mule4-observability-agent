package org.mule.extension.otel.mule4.observablity.agent.internal.notification;

import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;

import org.mule.runtime.api.notification.EnrichedServerNotification;
import org.mule.runtime.api.notification.MessageProcessorNotification;
import org.mule.runtime.api.notification.PipelineMessageNotification;
import org.mule.runtime.core.api.config.MuleConfiguration;
import org.mule.extension.otel.mule4.observablity.agent.internal.config.advanced.CustomAttributesConfig;
import org.mule.extension.otel.mule4.observablity.agent.internal.config.advanced.SpanGenerationConfig;
import org.mule.extension.otel.mule4.observablity.agent.internal.connection.OtelSdkConnection;
import org.mule.extension.otel.mule4.observablity.agent.internal.metric.MuleMetricMemoryUsage;
import org.mule.extension.otel.mule4.observablity.agent.internal.metric.MuleMetricSystemWorkload;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.NotificationParserUtils;
import org.mule.extension.otel.mule4.observablity.agent.internal.notification.parser.service.NotificationParserService;
import org.mule.extension.otel.mule4.observablity.agent.internal.notification.parser.service.provider.BaseNotificationParser;
import org.mule.extension.otel.mule4.observablity.agent.internal.notification.parser.service.provider.NotificationParser;
import org.mule.extension.otel.mule4.observablity.agent.internal.store.config.MuleConnectorConfigStore;
import org.mule.extension.otel.mule4.observablity.agent.internal.store.trace.MuleSoftTraceStore;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.function.Supplier;
import java.time.Duration; 

import org.mule.extension.otel.mule4.observablity.agent.internal.metric.MuleMetricErrors;
import org.mule.extension.otel.mule4.observablity.agent.internal.metric.MuleMetricLatency;
import org.mule.extension.otel.mule4.observablity.agent.internal.metric.MuleMetricTraffic;

public class OTelMuleNotificationHandler
{
	private static Logger logger = LoggerFactory.getLogger(OTelMuleNotificationHandler.class);

	private static MuleSoftTraceStore traceStore = new MuleSoftTraceStore();
	
	private OtelSdkConnection otelSdkConnection;
    private MuleConnectorConfigStore muleConnectorConfigStore;
	private final Supplier<OtelSdkConnection> sdkConnectionSupplier;

	// --------------------------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------------------------
	public OTelMuleNotificationHandler(Supplier<OtelSdkConnection> s)
	{
		sdkConnectionSupplier = s;
	}

	/**
	 * 
	 * @return traceStore - a reference to the MuleSoftTraceStore created and used by this class
	 */
	public static MuleSoftTraceStore getMuleSoftTraceStore()
	{
		return traceStore;
	}
	
	// --------------------------------------------------------------------------------------------
	// Helper methods to retrieve various singletons.
	// --------------------------------------------------------------------------------------------	
	private Tracer getTracer()
	{
		if (otelSdkConnection == null)
		{
			otelSdkConnection = sdkConnectionSupplier.get();
		}
		return otelSdkConnection.getTracer().get();
	}

	private MuleConfiguration getMuleConfiguration()
	{
		if (otelSdkConnection == null)
		{
			otelSdkConnection = sdkConnectionSupplier.get();
		}
		return otelSdkConnection.getMuleConfiguration().get();
	}
	
	private SpanGenerationConfig getSpanGenerationConfig()
	{
		if (otelSdkConnection == null)
		{
			otelSdkConnection = sdkConnectionSupplier.get();
		}
		return otelSdkConnection.getSpanGenerationConfig().get();
	}
	
	private MuleConnectorConfigStore getMuleConnectorConfigStore()
	{
	    if (muleConnectorConfigStore == null)
	    {
	        if (otelSdkConnection == null)
	        {
	            otelSdkConnection = sdkConnectionSupplier.get();
	        }

	        muleConnectorConfigStore = MuleConnectorConfigStore.getInstance(getMuleConfiguration(), 
	                                                                        otelSdkConnection.getExpressionManager().get());
	    }

	    return muleConnectorConfigStore;
	}
	
	private void setCustomAttributes(SpanBuilder sb, EnrichedServerNotification n, int action)
	{
	    if (otelSdkConnection == null)
	    {
	        otelSdkConnection = sdkConnectionSupplier.get();
	    }
	    
	    CustomAttributesConfig cac =  otelSdkConnection.getCustomAttributesConfig().get();
	    Boolean setAttributes = true;
	    
	    switch (action)
	    {
	        case Constants.PIPELINE_EVENT_ACTION_ID:
	            setAttributes = cac.getSendCustomAttributesPerFlow();
	            break;
	        case Constants.PROCESSOR_EVENT_ACTION_ID:
	            setAttributes = cac.getSendCustomAttributesPerProcessor();
	            break;
	    }
	    
	    if (setAttributes)
	        cac.setAttributes(sb, otelSdkConnection.getExpressionManager().get(), n);
	}
	
	// ============================================================================================
	//                      PIPELINE/FLOW RELATED NOTIFICATION EVENTS
	// ============================================================================================

	// --------------------------------------------------------------------------------------------
	// Flow START Notification Handler
	// --------------------------------------------------------------------------------------------	
	public void handleFlowStartEvent(PipelineMessageNotification notification)
	{
		logger.debug("Handling flow start event");

		Instant startInstant = NotificationParserUtils.getInstantFrom(notification);
		
		// Record Flow Traffic (NEW)
        MuleMetricTraffic.getInstance().recordFlowStart();
        
		SpanBuilder spanBuilder = getTracer().spanBuilder(NotificationParserUtils.getSpanName(notification))
				                                                                 .setStartTimestamp(startInstant);
		
		spanBuilder.setAttribute(Constants.START_DATETIME_ATTRIBUTE, startInstant.toString());
	    
		String workload = (MuleMetricSystemWorkload.getWorkloadPercent() >= 0) 
		                  ? String.format("%.2f %%", MuleMetricSystemWorkload.getWorkloadPercent())
		                  : "Data not available";
	    /*    
		spanBuilder.setAttribute(Constants.START_WORKLOAD_ATTRIBUTE, 
		                         String.format("%.2f %%", MuleMetricSystemWorkload.getWorkloadPercent()));
		*/
		
	    spanBuilder.setAttribute(Constants.START_WORKLOAD_ATTRIBUTE, workload);
		
	    spanBuilder.setAttribute(Constants.START_HEAP_USAGE_ATTRIBUTE, 
                                 String.format("%.2f MB", MuleMetricMemoryUsage.getHeapMemoryUsage()/1000000.0));
	    
	    //
	    // add custom attributes to the trace
	    //
	    setCustomAttributes(spanBuilder, notification, Constants.PIPELINE_EVENT_ACTION_ID);
	    
	      
		NotificationParser notificationParser = NotificationParserService.getInstance().getParserFor(notification)
				                                                                       .orElse(new BaseNotificationParser());
		
		if (!traceStore.isTracePresent(NotificationParserUtils.getMuleSoftTraceId(notification)))
		{
			try
			{
				spanBuilder.setAttribute(Constants.FLOW_NAME_ATTRIBUTE, NotificationParserUtils.getDocName(notification));
				spanBuilder.setAttribute(Constants.SERVER_ID_ATTRIBUTE, NotificationParserUtils.getServerId(notification));
				
				notificationParser.startPipelineNotification(notification, getMuleConnectorConfigStore(), spanBuilder);
			} 
			catch (Exception e)
			{
				logger.debug(e.getMessage());
			}
			
			traceStore.startTrace(NotificationParserUtils.getMuleSoftTraceId(notification),
		              NotificationParserUtils.getFlowId(notification),
		              spanBuilder.startSpan(),
                    startInstant);
		} 
		else
		{
			try
			{
				spanBuilder.setAttribute(Constants.DOC_NAME_ATTRIBUTE, NotificationParserUtils.getDocName(notification));
			} 
			catch (Exception e)
			{
				logger.debug(e.getMessage());
			}
			
			traceStore.addPipelineSpan(NotificationParserUtils.getMuleSoftTraceId(notification),
	                   NotificationParserUtils.getFlowId(notification),
	                   spanBuilder,
                    startInstant); // PASS START INSTANT HERE
		}
	}

	// --------------------------------------------------------------------------------------------
	// Flow END Notification Handler
	// --------------------------------------------------------------------------------------------
	public void handleFlowEndEvent(PipelineMessageNotification notification)
	{
		logger.debug("Handling flow end event");

		String mulesoftTraceId = NotificationParserUtils.getMuleSoftTraceId(notification);
        String flowId = NotificationParserUtils.getFlowId(notification);
        Instant endInstant = NotificationParserUtils.getInstantFrom(notification);
        Exception flowException = notification.getException();
        String flowName = NotificationParserUtils.getDocName(notification);


        // Record Flow Latency (NEW)
        Instant flowStartInstant = traceStore.getPipelineStartInstant(mulesoftTraceId, flowId);
        if (flowStartInstant != null) {
            long durationMs = Duration.between(flowStartInstant, endInstant).toMillis();
            MuleMetricLatency.getInstance().recordFlowLatency(durationMs, flowName);
        } else {
            logger.warn("Could not retrieve start instant for flow: {} ({}) to record latency.", flowName, flowId);
        }

        // Record Flow Error if an exception occurred (NEW)
        if (flowException != null) {
            MuleMetricErrors.getInstance().recordFlowError();
            logger.debug("Recorded flow error for flow: {}", flowName);
        }

		traceStore.endPipelineSpan(mulesoftTraceId,
				                   flowId, // Use flowId here
				                   endInstant,
				                   flowException); // Pass the exception
		
		if (traceStore.isPipelineSpansEmpty(mulesoftTraceId))
		{
			traceStore.endTrace(mulesoftTraceId);
		}
	}

	// ============================================================================================
	//                     MESSAGE PROCESSOR RELATED NOTIFICATION EVENTS
	// ============================================================================================

	// --------------------------------------------------------------------------------------------
	// Processor START Notification Handler
	// --------------------------------------------------------------------------------------------
	public void handleProcessorStartEvent(MessageProcessorNotification notification)
	{
		logger.debug("Handling processor start event");

		if (NotificationParserUtils.skipParsing(notification, getSpanGenerationConfig()))
			return;

        Instant startInstant = NotificationParserUtils.getInstantFrom(notification); // Get start time (NEW)

        // Record Processor Traffic (NEW)
        MuleMetricTraffic.getInstance().recordProcessorStart();

		NotificationParser notificationParser = NotificationParserService.getInstance()
				                                                         .getParserFor(notification)
                                                                         .orElse(new BaseNotificationParser());

		SpanBuilder spanBuilder = getTracer().spanBuilder(NotificationParserUtils.getSpanName(notification));

	    //
        // add custom attributes to the span
        //
        setCustomAttributes(spanBuilder, notification, Constants.PROCESSOR_EVENT_ACTION_ID);

		notificationParser.startProcessorNotification(notification, getMuleConnectorConfigStore(), spanBuilder);

        // Pass the startInstant to the traceStore (MODIFIED)
		traceStore.addMessageProcessorSpan(NotificationParserUtils.getMuleSoftTraceId(notification),
		                                   NotificationParserUtils.getFlowId(notification),
		                                   NotificationParserUtils.getSpanId(notification),
				                           spanBuilder,
                                           startInstant); // PASS START INSTANT HERE
	}

	// --------------------------------------------------------------------------------------------
	// Processor END Notification Handler
	// --------------------------------------------------------------------------------------------
	public void handleProcessorEndEvent(MessageProcessorNotification notification)
	{
		logger.debug("Handling end event");

		if (NotificationParserUtils.skipParsing(notification, getSpanGenerationConfig()))
			return;

        String mulesoftTraceId = NotificationParserUtils.getMuleSoftTraceId(notification);
        String flowId = NotificationParserUtils.getFlowId(notification);
        String spanId = NotificationParserUtils.getSpanId(notification);
        Instant endInstant = NotificationParserUtils.getInstantFrom(notification);
        Exception processorException = notification.getException();
        String componentId = NotificationParserUtils.getComponentId(notification);
        String docName = NotificationParserUtils.getDocName(notification);


        // Record Processor Latency (NEW)
        Instant processorStartInstant = traceStore.getMessageProcessorStartInstant(mulesoftTraceId, flowId, spanId);
        if (processorStartInstant != null) {
            long durationMs = Duration.between(processorStartInstant, endInstant).toMillis();
            MuleMetricLatency.getInstance().recordProcessorLatency(durationMs, componentId, docName);
        } else {
            logger.warn("Could not retrieve start instant for processor: {} ({}) to record latency.", docName, componentId);
        }

        // Record Processor Error if an exception occurred (NEW)
        if (processorException != null) {
            MuleMetricErrors.getInstance().recordProcessorError();
            logger.debug("Recorded processor error for processor: {}", docName);
        }

		NotificationParser notificationParser = NotificationParserService.getInstance()
				                                                         .getParserFor(notification)
                                                                         .orElse(new BaseNotificationParser());

		notificationParser.endProcessorNotification(notification, getMuleSoftTraceStore());

		traceStore.endMessageProcessorSpan(mulesoftTraceId,
                                           flowId, // Use flowId here
                                           spanId, // Use spanId here
                                           endInstant);
	}
}
