package org.mule.extension.otel.mule4.observablity.agent.internal.context.propagation;

import com.mulesoft.extension.mq.api.attributes.AnypointMQMessageAttributes;
import io.opentelemetry.context.propagation.TextMapGetter;

public class AnypointMQMessageAttributesGetter implements TextMapGetter<AnypointMQMessageAttributes>
{

	@Override
	public Iterable<String> keys(AnypointMQMessageAttributes carrier)
	{
		return carrier.getProperties().keySet();
	}

	@Override
	public String get(AnypointMQMessageAttributes carrier, String key)
	{
		for (String prop : carrier.getProperties().keySet()) {
			if (prop.equalsIgnoreCase(key)) {
				return (String) carrier.getProperties().get(prop);
			}
		}
		return null;
	}
}
