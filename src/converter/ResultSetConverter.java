package converter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import connector.OracleDBConnector;
import migrator.MongoDBMigrator;
import utils.Constants;

public class ResultSetConverter {

	public JSONObject convertResultSetToJSON(JSONObject metadata) {
		JSONObject response = null;
		try {
			// connect to oracle db
			String dbURL = metadata.get(Constants.INPUT_SOURCE).toString();
			String dbUserName = metadata.get(Constants.INPUT_SOURCE_LOGIN_USERNAME).toString();
			String dbPassword = metadata.get(Constants.INPUT_SOURCE_LOGIN_PASSWORD).toString();
			OracleDBConnector oracleDBConnector = new OracleDBConnector();
			Connection conn = oracleDBConnector.getConnection(dbURL, dbUserName, dbPassword);

			// extract the specified data
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
					columns.add(colName);
				}
				fetchQuery = fetchQuery.substring(0, fetchQuery.length() - 2);
				fetchQuery += " FROM " + tableName;
				System.out.println(fetchQuery);
				Statement statement = conn.createStatement();
				ResultSet resultSet = statement.executeQuery(fetchQuery);
				// convert to json
				JSONArray fetchedData = new JSONArray();
				while (resultSet.next()) {
					JSONObject dataRowObj = new JSONObject();
					for (int j = 1; j <= columns.size(); j++) {
						dataRowObj.put(columns.get(j - 1), resultSet.getString(j));
					}
					fetchedData.put(dataRowObj);
//					System.out.println(resultSet.getString(1) + " " + resultSet.getString(2) + " ");
				}
				System.out.println("asdasdasdas");
				System.out.println(fetchedData);
				// insert into mongo
//				MongoDBConnector.getConnection();
//				MongoDBMigrator.insertData();

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
