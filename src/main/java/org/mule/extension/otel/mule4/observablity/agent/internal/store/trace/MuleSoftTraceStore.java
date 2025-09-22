package org.mule.extension.otel.mule4.observablity.agent.internal.store.trace;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.api.trace.Span;
import org.mule.extension.http.api.HttpRequestAttributes;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//------------------------------------------------------------------------------------------------
//	Class for storing all MuleSoft traces/flows associated with a MuleSoft app. The class is a 
//	collection of MuleSoftTraces where each MuleSoftTrace has an associated rootSpan (i.e., the 
//	flow) and some number of child spans (i.e., a message processor - Logger, Transformer, ...).  
//	A new MuleSoftTrace is started whenever a flow is initiated by a Source (e.g., an HTTP Listener).
//
//  Causal relationship between segments of a MuleSoft trace:
//
//			             [Trace Root Span A] <-- (the uber root span)
//                               |
//			     +---------------+---------------+
//               |                               |
//      [Pipeline Span B]               [Pipeline Span C] <-- (Spans B, C are `children` of A)
//               |                               |
//  [Message Processor Span D]                   |
//                               +---------------+----------------+
//                               |                                |
//                  [Message Processor Span E]     [Message Processor Span F]
//
//------------------------------------------------------------------------------------------------

public class MuleSoftTraceStore {
	@SuppressWarnings("unused")
	private static Logger logger = LoggerFactory.getLogger(MuleSoftTraceStore.class);

	// ------------------------------------------------------------------------
	// Collection of MuleSoftTraces persisted as a simple hash map
	// ------------------------------------------------------------------------
	private Map<String, MuleSoftTrace> muleSoftTraces = new ConcurrentHashMap<>();

	// ------------------------------------------------------------------------
	// Nested class for storing all MuleSoft spans (pipeline and message
	// processor) associated with a MuleSoft trace.
	// ------------------------------------------------------------------------
	private class MuleSoftTrace {
		private Span traceRootSpan;
		private Map<String, PipelineSpan> pipelineSpans = new ConcurrentHashMap<>();

		// START: MuleSoftTrace constructor change
		private MuleSoftTrace(String rootSpanId, Span rootSpan, Instant startInstant, HttpRequestAttributes attributes) {
			this.traceRootSpan = rootSpan;
			pipelineSpans.put(rootSpanId, new PipelineSpan(rootSpan, startInstant, attributes));
		}
		// END: MuleSoftTrace constructor change

		private Span getRootSpan() {
			return traceRootSpan;
		}

		private boolean isPipelineSpansEmpty() {
			return pipelineSpans.isEmpty();
		}

		// START: putPipelineSpan method change
		private void putPipelineSpan(String pipelineSpanId, Span pipelineSpan, Instant startInstant, HttpRequestAttributes attributes) {
			pipelineSpans.put(pipelineSpanId, new PipelineSpan(pipelineSpan, startInstant, attributes));
		}
		// END: putPipelineSpan method change

		private PipelineSpan removePipelineSpan(String pipelineSpanId) {
			return pipelineSpans.remove(pipelineSpanId);
		}

		private PipelineSpan getPipelineSpan(String pipelineSpanId) {
			return pipelineSpans.get(pipelineSpanId);
		}

		private void end() {
			pipelineSpans.forEach((id, pipelineSpan) -> pipelineSpan.end());
		}

		// --------------------------------------------------------------------
		// Nested class for storing all MuleSoft processor spans associated
		// with a MuleSoft pipeline span.
		// --------------------------------------------------------------------
		private class PipelineSpan {
			private Span pipelineRootSpan;
			private Instant pipelineStartInstant;
			// START: Add attributes field
			private HttpRequestAttributes attributes;
			// END: Add attributes field
			private Map<String, MessageProcessorSpan> messageProcessorSpans = new ConcurrentHashMap<>();

			// START: PipelineSpan constructor change
			private PipelineSpan(Span rootSpan, Instant startInstant, HttpRequestAttributes attributes) {
				this.pipelineRootSpan = rootSpan;
				this.pipelineStartInstant = startInstant;
				this.attributes = attributes;
			}
			// END: PipelineSpan constructor change

			public Instant getPipelineStartInstant() {
				return pipelineStartInstant;
			}

			// START: Add getAttributes method
			public HttpRequestAttributes getAttributes() {
				return attributes;
			}
			// END: Add getAttributes method

			private void putSpan(String spanId, Span span, Instant startInstant) {
				this.messageProcessorSpans.put(spanId, new MessageProcessorSpan(span, startInstant));
			}

			private MessageProcessorSpan getSpan(String spanID) {
				return this.messageProcessorSpans.get(spanID);
			}

			private Span getRootSpan() {
				return pipelineRootSpan;
			}

			private void endSpan(String spanId, Instant endInstant) {
				MessageProcessorSpan messageProcessorSpan = messageProcessorSpans.get(spanId);
				if (messageProcessorSpan != null) {
					messageProcessorSpan.end(endInstant);
				}
			}

			private void end() {
				end(Instant.now(), null);
			}

			private void end(Instant endInstant, Exception e) {
				messageProcessorSpans.forEach((id, processorSpan) -> processorSpan.end(endInstant));
				if (pipelineRootSpan != null) {
					if (e != null) {
						pipelineRootSpan.setStatus(StatusCode.ERROR, e.getMessage());
						pipelineRootSpan.recordException(e);
					}
					pipelineRootSpan.setAttribute(Constants.END_DATETIME_ATTRIBUTE, endInstant.toString());
					pipelineRootSpan.end(endInstant);
				}
			}

			private class MessageProcessorSpan {
				private Span processorSpan;
				private Instant processorStartInstant;

				private MessageProcessorSpan(Span span, Instant startInstant) {
					this.processorSpan = span;
					this.processorStartInstant = startInstant;
				}

				public Span getProcessorSpan() {
					return processorSpan;
				}

				public Instant getProcessorStartInstant() {
					return processorStartInstant;
				}

				public void end(Instant endInstant) {
					processorSpan.end(endInstant);
				}
			}
		}

	}

	// ------------------------------------------------------------------------
	// Various Public Helper Methods for MuleSoft Traces
	// ------------------------------------------------------------------------
	/**
	 * 
	 * @param mulesoftTraceId
	 * @return <b>true</b> if a trace with this id exists in the store; else false
	 */
	public boolean isTracePresent(String mulesoftTraceId) {
		return muleSoftTraces.containsKey(mulesoftTraceId);
	}

	/**
	 * 
	 * @param mulesoftTraceId
	 * @return <b>true</b> if there are no more pipeline spans associated with this
	 *         traceid, else false
	 */
	public boolean isPipelineSpansEmpty(String mulesoftTraceId) {
		return muleSoftTraces.get(mulesoftTraceId).isPipelineSpansEmpty();
	}

	/**
	 * Create a new MuleSoftTrace with id {@code mulesoftTraceid} and set
	 * {@code rootSpan} as the overall uber (root/parent) span
	 * 
	 * @param mulesoftTraceId - unique id for this trace
	 * @param rootSpanId      - unique id for the root (parent) span for this trace
	 * @param rootSpan        - the parent {@link Span}
	 */
	  // --------------------------------------------------------------------
    // startTrace Method
    // --------------------------------------------------------------------
    // START: startTrace changes
    public void startTrace(String mulesoftTraceId, String rootSpanId, Span rootSpan, Instant startInstant, HttpRequestAttributes attributes) {
        muleSoftTraces.put(mulesoftTraceId, new MuleSoftTrace(rootSpanId, rootSpan, startInstant, attributes));
    }
    // END: startTrace changes


	public Context getTraceContextFor(String mulesoftTraceId) {
		Context context = Context.current();

		if (isTracePresent(mulesoftTraceId)) {
			context = muleSoftTraces.get(mulesoftTraceId).getRootSpan().storeInContext(Context.current());
		}

		return context;
	}

	/**
	 * Remove the trace with id {@code mulesoftTraceid} from the store. Also, close
	 * off the trace by ending each span within the trace.
	 * 
	 * @param mulesoftTraceId - unique id for this trace
	 */
	public void endTrace(String mulesoftTraceId) {
		MuleSoftTrace muleSoftTrace = muleSoftTraces.remove(mulesoftTraceId);
		if (muleSoftTrace != null) {
			muleSoftTrace.end();
		}
	}

	  // --------------------------------------------------------------------
    // addPipelineSpan Method
    // --------------------------------------------------------------------
    // START: addPipelineSpan changes
    public void addPipelineSpan(String mulesoftTraceId, String pipelineId, SpanBuilder spanBuilder, Instant startInstant, HttpRequestAttributes attributes) {
        MuleSoftTrace muleSoftTrace = muleSoftTraces.get(mulesoftTraceId);
        if (muleSoftTrace != null) {
            Span newSpan = spanBuilder.setParent(Context.current().with(muleSoftTrace.getRootSpan())).startSpan();
            muleSoftTrace.putPipelineSpan(pipelineId, newSpan, startInstant, attributes);
        }
    }
    // END: addPipelineSpan changes

	// ADD NEW GETTER: To retrieve pipeline start instant for latency calculation
	public Instant getPipelineStartInstant(String mulesoftTraceId, String pipelineId) {
		MuleSoftTrace muleSoftTrace = muleSoftTraces.get(mulesoftTraceId);
		if (muleSoftTrace != null) {
			MuleSoftTrace.PipelineSpan pipelineSpan = muleSoftTrace.getPipelineSpan(pipelineId);
			if (pipelineSpan != null) {
				return pipelineSpan.getPipelineStartInstant();
			}
		}
		return null;
	}
 // --------------------------------------------------------------------
    // getHttpRequestAttributes Method
    // --------------------------------------------------------------------
    // START: getHttpRequestAttributes changes
    public HttpRequestAttributes getHttpRequestAttributes(String mulesoftTraceId, String pipelineId) {
        MuleSoftTrace muleSoftTrace = muleSoftTraces.get(mulesoftTraceId);
        if (muleSoftTrace != null) {
            MuleSoftTrace.PipelineSpan pipelineSpan = muleSoftTrace.getPipelineSpan(pipelineId);
            if (pipelineSpan != null) {
                return pipelineSpan.getAttributes();
            }
        }
        return null;
    }
    // END: getHttpRequestAttributes changes
	public void endPipelineSpan(String mulesoftTraceId, String pipelineId) {
		endPipelineSpan(mulesoftTraceId, pipelineId, Instant.now(), null);
	}

	public void endPipelineSpan(String mulesoftTraceId, String pipelineId, Instant endInstant, Exception e) {
		MuleSoftTrace muleSoftTrace = muleSoftTraces.get(mulesoftTraceId);
		if (muleSoftTrace != null) {
			MuleSoftTrace.PipelineSpan pipelineSpan = muleSoftTrace.removePipelineSpan(pipelineId);
			if (pipelineSpan != null) {
				pipelineSpan.end(endInstant, e);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Helper Methods for Message Processor Spans
	// ------------------------------------------------------------------------
	public void addMessageProcessorSpan(String mulesoftTraceId, String pipelineId, String messageProcessorId, SpanBuilder spanBuilder, Instant startInstant) {
		MuleSoftTrace muleSoftTrace = muleSoftTraces.get(mulesoftTraceId);
		if (muleSoftTrace != null) {
			MuleSoftTrace.PipelineSpan pipelineSpan = muleSoftTrace.getPipelineSpan(pipelineId);
			if (pipelineSpan != null) {
				Span newMessageProcessorSpan = spanBuilder.setParent(Context.current().with(pipelineSpan.getRootSpan())).startSpan();
				pipelineSpan.putSpan(messageProcessorId, newMessageProcessorSpan, startInstant);
			}
		}
	}

// ADD NEW GETTER: To retrieve message processor start instant for latency calculation
	public Instant getMessageProcessorStartInstant(String mulesoftTraceId, String pipelineId, String messageProcessorId) {
		MuleSoftTrace muleSoftTrace = muleSoftTraces.get(mulesoftTraceId);
		if (muleSoftTrace != null) {
			MuleSoftTrace.PipelineSpan pipelineSpan = muleSoftTrace.getPipelineSpan(pipelineId);
			if (pipelineSpan != null) {
				MuleSoftTrace.PipelineSpan.MessageProcessorSpan mpSpan = pipelineSpan.getSpan(messageProcessorId);
				if (mpSpan != null) {
					return mpSpan.getProcessorStartInstant();
				}
			}
		}
		return null;
	}

	public Span getMessageProcessorSpan(String mulesoftTraceId, String pipelineId, String messageProcessorId) {
	    MuleSoftTrace trace = muleSoftTraces.get(mulesoftTraceId);
	    if (trace == null) {
	        logger.warn("No MuleSoftTrace found for traceId: " + mulesoftTraceId);
	        return null;
	    }
	    MuleSoftTrace.PipelineSpan pipelineSpan = trace.getPipelineSpan(pipelineId);
	    if (pipelineSpan == null) {
	        logger.warn("No PipelineSpan found for pipelineId: " + pipelineId + " in traceId: " + mulesoftTraceId);
	        return null;
	    }
	    MuleSoftTrace.PipelineSpan.MessageProcessorSpan spanWrapper = pipelineSpan.getSpan(messageProcessorId);
	    if (spanWrapper == null) {
	        logger.warn("No MessageProcessorSpan found for messageProcessorId: " + messageProcessorId + " in pipelineId: " + pipelineId);
	        return null;
	    }
	    return spanWrapper.getProcessorSpan();
	}

	public void endMessageProcessorSpan(String mulesoftTraceId, String pipelineId, String messageProcessorId, Instant endInstant) {
		MuleSoftTrace muleSoftTrace = muleSoftTraces.get(mulesoftTraceId);
		if (muleSoftTrace != null) {
			MuleSoftTrace.PipelineSpan pipelineSpan = muleSoftTrace.getPipelineSpan(pipelineId);
			if (pipelineSpan != null) {
				pipelineSpan.endSpan(messageProcessorId, endInstant);
			}
		}
	}
}
