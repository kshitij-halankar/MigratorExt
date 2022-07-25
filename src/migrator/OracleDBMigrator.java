package migrator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import connector.OracleDBConnector;
import utils.Constants;

public class OracleDBMigrator {

	public int[] insertCSVData(JSONObject metadata, JSONObject entity, CSVReader csvReader, String sql,
			List<Integer> columnNumber, String tableName) throws SQLException, CsvValidationException, IOException {
		// convert CSV to SQL
		String[] row = null;

		String dbURL = metadata.get(Constants.OUTPUT_SOURCE).toString();
		String dbUserName = metadata.get(Constants.OUTPUT_SOURCE_LOGIN_USERNAME).toString();
		String dbPassword = metadata.get(Constants.OUTPUT_SOURCE_LOGIN_PASSWORD).toString();

		OracleDBConnector oracleDBConnector = new OracleDBConnector();
		Connection conn = oracleDBConnector.getConnection(dbURL, dbUserName, dbPassword);
		ResultSetMetaData tableMetadata = getTableMetadata(metadata, tableName);

//		System.out.println("tableMetadata: "+tableMetadata.getColumnCount());
		JSONObject dataTypes = new JSONObject();
		for (int k = 1; k <= tableMetadata.getColumnCount(); k++) {
//			System.out.println("tableMetadata: "+tableMetadata.getColumnName(k)+" : "+tableMetadata.getColumnTypeName(k));
			dataTypes.put(tableMetadata.getColumnName(k), tableMetadata.getColumnTypeName(k));
		}
		PreparedStatement p = conn.prepareStatement(sql);
		int batchSize = 0;
		int[] insertResult = null;
		while ((row = csvReader.readNext()) != null) {
			/*
			 * iterate array to only select required columns
			 */
			for (int j = 1; j <= columnNumber.size(); j++) {
				p.setString(j, row[columnNumber.get(j - 1)]);
				System.out.print(row[columnNumber.get(j - 1)] + ", ");
			}
			System.out.println();
//            for (int j = 1; j < row.length; j++) {
//                p.setString(j, row[j]);
//            }
			p.addBatch();
			p.clearParameters();
			batchSize++;
			if (batchSize == Constants.BATCH_SIZE) {
				insertResult = p.executeBatch();
				batchSize = 0;
			}
		}
		if (batchSize > 0) {
			insertResult = p.executeBatch();
		}
		return insertResult;
	}

	public int[] insertJSONData(JSONObject metadata, JSONArray dataArray, List<String> mappingAttributes, String sql,
			String entity) throws SQLException {
		int[] insertResult = null;
		String dbURL = metadata.get(Constants.OUTPUT_SOURCE).toString();
		String dbUserName = metadata.get(Constants.OUTPUT_SOURCE_LOGIN_USERNAME).toString();
		String dbPassword = metadata.get(Constants.OUTPUT_SOURCE_LOGIN_PASSWORD).toString();
		OracleDBConnector oracleDBConnector = new OracleDBConnector();
		PreparedStatement p = oracleDBConnector.getConnection(dbURL, dbUserName, dbPassword).prepareStatement(sql);
		int batchSize = 0;

		JSONArray entities = metadata.getJSONObject(Constants.SCHEMA).getJSONArray(Constants.ENTITIES);

		JSONArray mappings = null;
		for (int m = 0; m < entities.length(); m++) {
			if (entities.getJSONObject(m).getString(Constants.INPUT_ENTITY_NAME).equals(entity)) {
				mappings = entities.getJSONObject(m).getJSONArray(Constants.MAPPINGS);
			}
			String tableName=entities.getJSONObject(m).getString(Constants.OUTPUT_ENTITY_NAME);
			ResultSetMetaData tableMetadata = getTableMetadata(metadata, tableName);

//		System.out.println("tableMetadata: "+tableMetadata.getColumnCount());
			JSONObject dataTypes = new JSONObject();
			for (int k = 1; k <= tableMetadata.getColumnCount(); k++) {
//			System.out.println("tableMetadata: "+tableMetadata.getColumnName(k)+" : "+tableMetadata.getColumnTypeName(k));
				dataTypes.put(tableMetadata.getColumnName(k), tableMetadata.getColumnTypeName(k));
			}
			System.out.println(dataTypes);
//        JSONObject mappingAttributes = new JSONObject();
//        for (int k = 0; k < mappings.length(); k++) {
//            mappingAttributes.put(mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME),
//                    mappings.getJSONObject(k).getString(Constants.OUTPUT_ATTRIBUTE_NAME));
//        }
			for (int i = 0; i < dataArray.length(); i++) {
				JSONObject dataRow = dataArray.getJSONObject(i);
				for (int j = 1; j <= mappingAttributes.size(); j++) {
					for (int k = 0; k < mappings.length(); k++) {
//                    System.out.println("test "+mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME));
						if (mappingAttributes.get(j - 1)
								.equals(mappings.getJSONObject(k).getString(Constants.OUTPUT_ATTRIBUTE_NAME))) {
//                        System.out.println("mapping: " + mappingAttributes.get(j - 1) + " - "+ mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME));
							String attributeName = mappings.getJSONObject(k).getString(Constants.OUTPUT_ATTRIBUTE_NAME);
							String dataType = dataTypes.getString(attributeName.toUpperCase());
							if (dataType.equalsIgnoreCase("NUMBER")) {
								p.setInt(j, dataRow
										.getInt(mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME)));
							} else {
								p.setString(j, dataRow
										.get(mappings.getJSONObject(k).get(Constants.INPUT_ATTRIBUTE_NAME).toString())
										.toString());
							}

//                        System.out.println("dataRow: " + dataRow.getString(mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME)));
							break;
						}
//                    System.out.println(p.toString());
					}
					System.out.println();
				}
//            JSONObject insertRow = new JSONObject();
//            Iterator<String> dataRowKeys = dataRow.keys();
//            int cols = 1;
//            while (dataRowKeys.hasNext()) {
//                String key = dataRowKeys.next();
//                if (mappingAttributes.(key)) {
////                    insertRow.put(key, dataRow.getString(key));
//                    p.setString(cols, dataRow.getString(key));
//                    cols++;
//                }
//            }

				p.addBatch();
				p.clearParameters();
				batchSize++;
				if (batchSize == Constants.BATCH_SIZE) {
					insertResult = p.executeBatch();
					batchSize = 0;
				}
			}
			if (batchSize > 0) {
				insertResult = p.executeBatch();
			}
		}
		return insertResult;
	}

	public int[] insertXMLData(JSONObject metadata, String sql, String entity) throws SQLException {
		int[] insertResult = null;
		String dbURL = metadata.get(Constants.OUTPUT_SOURCE).toString();
		String dbUserName = metadata.get(Constants.OUTPUT_SOURCE_LOGIN_USERNAME).toString();
		String dbPassword = metadata.get(Constants.OUTPUT_SOURCE_LOGIN_PASSWORD).toString();
		OracleDBConnector oracleDBConnector = new OracleDBConnector();
		PreparedStatement p = oracleDBConnector.getConnection(dbURL, dbUserName, dbPassword).prepareStatement(sql);
		int batchSize = 0;

		JSONArray entities = metadata.getJSONObject(Constants.SCHEMA).getJSONArray(Constants.ENTITIES);

		JSONArray mappings = null;
		for (int i = 0; i < entities.length(); i++) {
			if (entities.getJSONObject(i).getString(Constants.INPUT_ENTITY_NAME).equals(entity)) {
				mappings = entities.getJSONObject(i).getJSONArray(Constants.MAPPINGS);
			}
		}

		return insertResult;
	}

	public ResultSetMetaData getTableMetadata(JSONObject metadata, String entityName) throws SQLException {
		ResultSetMetaData tableMetaData = null;
		String dbURL = metadata.get(Constants.OUTPUT_SOURCE).toString();
		String dbUserName = metadata.get(Constants.OUTPUT_SOURCE_LOGIN_USERNAME).toString();
		String dbPassword = metadata.get(Constants.OUTPUT_SOURCE_LOGIN_PASSWORD).toString();
		OracleDBConnector oracleDBConnector = new OracleDBConnector();
		Connection con = oracleDBConnector.getConnection(dbURL, dbUserName, dbPassword);
		Statement stmt = con.createStatement();
		System.out.println(entityName);
		ResultSet rs = stmt.executeQuery("select * from " + entityName + " where ROWNUM <= 1");
		tableMetaData = rs.getMetaData();
		return tableMetaData;
	}

	public ResultSetMetaData getTableMetadataFromInput(JSONObject metadata, String entityName) throws SQLException {
		ResultSetMetaData tableMetaData = null;
		String dbURL = metadata.get(Constants.INPUT_SOURCE).toString();
		String dbUserName = metadata.get(Constants.INPUT_SOURCE_LOGIN_USERNAME).toString();
		String dbPassword = metadata.get(Constants.INPUT_SOURCE_LOGIN_PASSWORD).toString();
		OracleDBConnector oracleDBConnector = new OracleDBConnector();
		Connection con = oracleDBConnector.getConnection(dbURL, dbUserName, dbPassword);
		Statement stmt = con.createStatement();
		System.out.println(entityName);
		ResultSet rs = stmt.executeQuery("select * from " + entityName + " where ROWNUM <= 1");
		tableMetaData = rs.getMetaData();
		return tableMetaData;
	}
}