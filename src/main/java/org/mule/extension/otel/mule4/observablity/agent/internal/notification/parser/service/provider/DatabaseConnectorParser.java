package org.mule.extension.otel.mule4.observablity.agent.internal.notification.parser.service.provider;

import java.util.Map;
import org.mule.extension.otel.mule4.observablity.agent.internal.store.config.MuleConnectorConfigStore;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.Constants;
import org.mule.extension.otel.mule4.observablity.agent.internal.util.NotificationParserUtils;
import org.mule.runtime.api.notification.EnrichedServerNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.SpanBuilder;

public class DatabaseConnectorParser extends BaseNotificationParser
{
	private static Logger logger = LoggerFactory.getLogger(DatabaseConnectorParser.class);

	// --------------------------------------------------------------------------------------------
	// Verify if this Parser can handle this notification
	// --------------------------------------------------------------------------------------------    
	@Override
	public boolean canParse(EnrichedServerNotification notification)
	{
		return NotificationParserUtils.getComponentId(notification).matches(Constants.DB_MATCHER);
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

		return addDatabaseAttributesToSpan(notification, muleConnectorConfigStore, spanBuilder);
	}

	// --------------------------------------------------------------------------------------------
	// Annotate the span with various Database attributes
	// --------------------------------------------------------------------------------------------
	private SpanBuilder addDatabaseAttributesToSpan(EnrichedServerNotification notification,
													MuleConnectorConfigStore muleConnectorConfigStore,
													SpanBuilder spanBuilder)
	{        
		Map<String, String> componentParameters = NotificationParserUtils.getComponentAnnotation("{config}componentParameters", notification);
		String sql = componentParameters.get("sql");
		String configRef = componentParameters.get("config-ref");

		MuleConnectorConfigStore.DbConfig dbConfig = muleConnectorConfigStore.getConfig(configRef);

		try
		{
			// OTEL semantic conventions and New Relic guidelines
			spanBuilder.setAttribute("db.system", dbConfig.getConnectionType()); // e.g., "mysql"
			spanBuilder.setAttribute("db.name", dbConfig.getDbName());
			spanBuilder.setAttribute("db.user", dbConfig.getUser());
			spanBuilder.setAttribute("db.statement", sql); // OTEL: full SQL statement
			spanBuilder.setAttribute("net.peer.name", dbConfig.getHost());
			spanBuilder.setAttribute("net.peer.port", dbConfig.getPort());

			// Try to extract db.operation and db.sql.table if possible
			String dbOperation = extractDbOperation(sql);
			if (dbOperation != null) {
				spanBuilder.setAttribute("db.operation", dbOperation);
			}
			String dbSqlTable = extractDbSqlTable(sql);
			if (dbSqlTable != null) {
				spanBuilder.setAttribute("db.sql.table", dbSqlTable);
			}
		}
		catch (Exception e)
		{
			logger.debug(e.getMessage());
		}
		return spanBuilder;
	}

	// Extracts the SQL operation (e.g., SELECT, INSERT)
	private String extractDbOperation(String sql) {
		if (sql == null) return null;
		String trimmed = sql.trim().toUpperCase();
		String[] tokens = trimmed.split("\\s+");
		if (tokens.length > 0) {
			return tokens[0];
		}
		return null;
	}

	// Extracts the table name from a simple SQL statement
	private String extractDbSqlTable(String sql) {
		if (sql == null) return null;
		String upperSql = sql.toUpperCase();
		String table = null;
		if (upperSql.startsWith("SELECT")) {
			int fromIdx = upperSql.indexOf(" FROM ");
			if (fromIdx != -1) {
				String afterFrom = sql.substring(fromIdx + 6).trim();
				String[] tokens = afterFrom.split("\\s+");
				if (tokens.length > 0) {
					table = tokens[0].replaceAll("[^a-zA-Z0-9_]", "");
				}
			}
		} else if (upperSql.startsWith("INSERT INTO")) {
			int intoIdx = upperSql.indexOf("INTO ");
			if (intoIdx != -1) {
				String afterInto = sql.substring(intoIdx + 5).trim();
				String[] tokens = afterInto.split("\\s+|\\(");
				if (tokens.length > 0) {
					table = tokens[0].replaceAll("[^a-zA-Z0-9_]", "");
				}
			}
		} else if (upperSql.startsWith("UPDATE")) {
			String afterUpdate = sql.substring(6).trim();
			String[] tokens = afterUpdate.split("\\s+");
			if (tokens.length > 0) {
				table = tokens[0].replaceAll("[^a-zA-Z0-9_]", "");
			}
		} else if (upperSql.startsWith("DELETE FROM")) {
			int fromIdx = upperSql.indexOf("FROM ");
			if (fromIdx != -1) {
				String afterFrom = sql.substring(fromIdx + 5).trim();
				String[] tokens = afterFrom.split("\\s+");
				if (tokens.length > 0) {
					table = tokens[0].replaceAll("[^a-zA-Z0-9_]", "");
				}
			}
		}
		return table;
	}
}
