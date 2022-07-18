package converter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.PreparedStatement;

import org.json.JSONArray;
import org.json.JSONObject;

import com.opencsv.CSVReader;

import connector.OracleDBConnector;
import migrator.OracleDBMigrator;
import utils.Constants;

public class CSVConverter {

	public JSONObject convertCSVToJSON(JSONObject metadata, StringBuilder fileData) {
		JSONObject result = null;
		return result;
	}

	public JSONObject convertCSVToSQLAndInsert(JSONObject metadata) {
		JSONObject response = null;
		String sql = null;
		try {
//			System.out.println(metadata);
			String inputFile = metadata.get(Constants.INPUT_SOURCE).toString();
//			System.out.println("inputFile: " + inputFile);
//			BufferedReader lineReader = new BufferedReader(new FileReader(inputFile));
			CSVReader csvReader = new CSVReader(new FileReader(inputFile));
			String lineText = null;
			int count = 0;
//			lineText = lineReader.readLine();
//			System.out.println(lineText);
			String headers[] = csvReader.readNext();
//			System.out.println(headers.length);
//			for(int k=0;k<headers.length;k++) {
//				System.out.println(headers[k]);
//			}

			JSONObject schema = metadata.getJSONObject(Constants.SCHEMA);
			JSONArray entities = schema.getJSONArray(Constants.ENTITIES);
			for (int i = 0; i < entities.length(); i++) {
				JSONObject entity = entities.getJSONObject(i);
				JSONArray mappings = entity.getJSONArray(Constants.MAPPINGS);
				String tableName = entity.getString(Constants.OUTPUT_ENTITY_NAME);
				sql = Constants.SQL_INSERT + tableName;
				sql += "(";
				String columns = "";
				int columnCount = 0;
				for (String attribute : headers) {
//					System.out.println(attribute);
					for (int j = 0; j < mappings.length(); j++) {
						if (mappings.getJSONObject(j).getString(Constants.INPUT_ATTRIBUTE_NAME).equals(attribute)) {
							columns += mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME) + ", ";
							columnCount++;
						}
					}
				}
				columns = columns.substring(0, columns.length() - 2);
				String values = "";
				for (int j = 0; j < columnCount; j++) {
					values += "?, ";
				}
				values = values.substring(0, values.length() - 2);
				sql += columns + ")" + Constants.SQL_VALUES + "(" + values + ")";
				System.out.println("sql: " + sql);

				OracleDBMigrator oracleDBMigrator = new OracleDBMigrator();
				oracleDBMigrator.insertCSVData(entity, csvReader, sql);

			}
//			lineReader.close();
			csvReader.close();

		} catch (Exception ex) {
			response = new JSONObject();
			response.put(Constants.MIGRATION_STATUS, "failed");
			response.put(Constants.FAILURE_CAUSE, ex.toString());
		}
		return response;
	}
}
