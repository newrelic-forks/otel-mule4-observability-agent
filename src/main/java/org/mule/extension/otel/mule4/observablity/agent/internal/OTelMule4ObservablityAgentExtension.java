package org.mule.extension.otel.mule4.observablity.agent.internal;

import org.mule.runtime.extension.api.annotation.Extension;
import org.mule.extension.otel.mule4.observablity.agent.internal.config.OTelMule4ObservablityAgentConfiguration;
import org.mule.runtime.extension.api.annotation.Configurations;
import org.mule.runtime.extension.api.annotation.dsl.xml.Xml;
import static org.mule.sdk.api.meta.JavaVersion.JAVA_11;
import static org.mule.sdk.api.meta.JavaVersion.JAVA_17;
import static org.mule.sdk.api.meta.JavaVersion.JAVA_8;
import org.mule.sdk.api.annotation.JavaVersionSupport;
/**
 * This is the main class of an extension, is the entry point from which
 * configurations, connection providers, operations and sources are going to be
 * declared.
 * <p>
 * @see
 * <a href="https://github.com/open-telemetry/opentelemetry-java">OpenTelemetry Java Repository</a>
 */
@Xml(prefix = "otel-mule4-observablity-agent")
@Extension(name = "OpenTelemetry Mule 4 Observablity Agent", vendor ="MuleSoft")
@JavaVersionSupport({JAVA_17})
@Configurations(OTelMule4ObservablityAgentConfiguration.class)
public class OTelMule4ObservablityAgentExtension
{

}