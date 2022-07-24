package migrator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
			List<Integer> columnNumber) throws SQLException, CsvValidationException, IOException {
		// convert CSV to SQL
		String[] row = null;

		String dbURL = metadata.get(Constants.OUTPUT_SOURCE).toString();
		String dbUserName = metadata.get(Constants.OUTPUT_SOURCE_LOGIN_USERNAME).toString();
		String dbPassword = metadata.get(Constants.OUTPUT_SOURCE_LOGIN_PASSWORD).toString();

		OracleDBConnector oracleDBConnector = new OracleDBConnector();
		Connection conn = oracleDBConnector.getConnection(dbURL, dbUserName, dbPassword);
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
//			for (int j = 1; j < row.length; j++) {
//				p.setString(j, row[j]);
//			}
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

	public int[] insertJSONData(JSONObject metadata, JSONArray dataArray, String sql, String entity)
			throws SQLException {
		int[] insertResult = null;
		String dbURL = metadata.get(Constants.INPUT_SOURCE).toString();
		String dbUserName = metadata.get(Constants.INPUT_SOURCE_LOGIN_USERNAME).toString();
		String dbPassword = metadata.get(Constants.INPUT_SOURCE_LOGIN_PASSWORD).toString();
		OracleDBConnector oracleDBConnector = new OracleDBConnector();
		PreparedStatement p = oracleDBConnector.getConnection(dbURL, dbUserName, dbPassword).prepareStatement(sql);
		int batchSize = 0;

		JSONArray entities = metadata.getJSONObject(Constants.SCHEMA).getJSONArray(Constants.ENTITIES);

		JSONArray mappings;
		for (int i = 0; i < entities.length(); i++) {
			if (entities.getJSONObject(i).getString(Constants.INPUT_ENTITY_NAME).equals(entity)) {
				mappings = entities.getJSONObject(i).getJSONArray(Constants.MAPPINGS);
			}
		}
		List<String> mappingAttributes = new ArrayList<>();
		for (int i = 0; i < dataArray.length(); i++) {
			JSONObject dataRow = dataArray.getJSONObject(i);
//			JSONObject insertRow = new JSONObject();
			Iterator<String> dataRowKeys = dataRow.keys();
			int cols = 1;
			while (dataRowKeys.hasNext()) {
				String key = dataRowKeys.next();
				if (mappingAttributes.contains(key)) {
//					insertRow.put(key, dataRow.getString(key));
					p.setString(cols, dataRow.getString(key));
					cols++;
				}
			}
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
}
