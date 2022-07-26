package converter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import connector.OracleDBConnector;
import migrator.MongoDBMigrator;
import migrator.OracleDBMigrator;
import utils.Constants;

public class ResultSetConverter {

	public JSONObject convertResultSetToJSON(JSONObject metadata) {
		JSONObject response = null;
		try {
			String dbURL = metadata.get(Constants.INPUT_SOURCE).toString();
			String dbUserName = metadata.get(Constants.INPUT_SOURCE_LOGIN_USERNAME).toString();
			String dbPassword = metadata.get(Constants.INPUT_SOURCE_LOGIN_PASSWORD).toString();
			OracleDBConnector oracleDBConnector = new OracleDBConnector();
			Connection conn = oracleDBConnector.getConnection(dbURL, dbUserName, dbPassword);
			OracleDBMigrator oracleDBMigrator = new OracleDBMigrator();
			JSONObject schema = metadata.getJSONObject(Constants.SCHEMA);
			JSONArray entities = schema.getJSONArray(Constants.ENTITIES);
			for (int i = 0; i < entities.length(); i++) {
				JSONObject entity = entities.getJSONObject(i);
				JSONArray mappings = entity.getJSONArray(Constants.MAPPINGS);
				String tableName = entity.getString(Constants.INPUT_ENTITY_NAME);
				String fetchQuery = "";
				fetchQuery = "Select ";
				List<String> columns = new ArrayList<>();
				for (int j = 0; j < mappings.length(); j++) {
					String colName = mappings.getJSONObject(j).getString(Constants.INPUT_ATTRIBUTE_NAME);
					fetchQuery += colName + ", ";
					columns.add(mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME));
				}
				fetchQuery = fetchQuery.substring(0, fetchQuery.length() - 2);
				fetchQuery += " FROM " + tableName;
				Statement statement = conn.createStatement();
				ResultSet resultSet = statement.executeQuery(fetchQuery);
				ResultSetMetaData tableMetadata = oracleDBMigrator.getTableMetadataFromInput(metadata, tableName);
				JSONObject dataTypes = new JSONObject();
				for (int k = 1; k <= tableMetadata.getColumnCount(); k++) {
					dataTypes.put(tableMetadata.getColumnName(k), tableMetadata.getColumnTypeName(k));
				}
				List<Document> fetchedData = new ArrayList<>();
				while (resultSet.next()) {
					Document tempObject = new Document();
					for (int j = 1; j <= columns.size(); j++) {
						String attributeName = mappings.getJSONObject(j - 1).getString(Constants.INPUT_ATTRIBUTE_NAME);
						String dataType = dataTypes.getString(attributeName.toUpperCase());
						if (dataType.equalsIgnoreCase("NUMBER")) {
							tempObject.append(columns.get(j - 1).toString(), resultSet.getInt(j));
						} else {
							tempObject.append(columns.get(j - 1).toString(), resultSet.getString(j));
						}
					}
					fetchedData.add(tempObject);
				}
				MongoDBMigrator mongoDBMigrator = new MongoDBMigrator();
				mongoDBMigrator.insertData(metadata, fetchedData);
			}

		} catch (Exception ex) {
			response = new JSONObject();
			response.put(Constants.MIGRATION_STATUS, "failed");
			response.put(Constants.FAILURE_CAUSE, ex.toString());
		}
		return response;
	}
}
