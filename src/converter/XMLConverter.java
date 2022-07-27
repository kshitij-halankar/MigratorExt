package converter;

import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import connector.FileExtractor;
import migrator.MongoDBMigrator;
import migrator.OracleDBMigrator;
import utils.Constants;

public class XMLConverter {

	public JSONObject convertXMLToJSON(JSONObject metadata, StringBuilder fileData) {
		JSONObject insertResponse = new JSONObject();
		int insertedRecordsCount = 0;
		int batchSize = 0;
		JSONObject schema = metadata.getJSONObject(Constants.SCHEMA);
		JSONArray entities = schema.getJSONArray(Constants.ENTITIES);
		JSONObject xmlTojson = XML.toJSONObject(fileData.toString());
		List<Document> menu = new ArrayList<>();
		for (int i = 0; i < entities.length(); i++) {
			JSONObject entity = entities.getJSONObject(i);
			JSONArray mappings = entity.getJSONArray(Constants.MAPPINGS);
			JSONObject test = xmlTojson.getJSONObject(schema.getString(Constants.INPUT_SCHEMA));
			JSONArray testJsonArray = test.getJSONArray(entity.getString(Constants.INPUT_ENTITY_NAME));
			Document food = new Document();
			for (int j = 0; j < testJsonArray.length(); j++) {
				food = new Document();
				for (int k = 0; k < mappings.length(); k++) {
					if (testJsonArray.getJSONObject(j)
							.has(mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME))) {
						food.append(mappings.getJSONObject(k).getString(Constants.OUTPUT_ATTRIBUTE_NAME),
								testJsonArray.getJSONObject(j)
										.get(mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME)));
					}
				}
				menu.add(food);
				batchSize++;
				if (batchSize == Constants.BATCH_SIZE) {
					MongoDBMigrator mongoMigrator = new MongoDBMigrator();
					insertedRecordsCount += mongoMigrator.insertData(metadata, menu);
					batchSize = 0;
					menu = new ArrayList<>();
				}
			}
			if (batchSize > 0) {
				MongoDBMigrator mongoMigrator = new MongoDBMigrator();
				insertedRecordsCount += mongoMigrator.insertData(metadata, menu);
			}
		}
		insertResponse.put(Constants.RESPONSE_STATUS, Constants.RESPONSE_SUCCESS);
		insertResponse.put(Constants.RESPONSE_TOTAL_RECORDS_INSERTED, insertedRecordsCount);
		return insertResponse;
	}

	public JSONObject convertAndInsertXMLToSQL(JSONObject metadata) {
		JSONObject insertResponse = new JSONObject();
		String sql = null;
		try {
			String inputFile = metadata.get(Constants.INPUT_SOURCE).toString();
			FileExtractor fileExtractor = new FileExtractor();
			JSONObject jsonData = XML.toJSONObject(fileExtractor.getFile(inputFile).toString());
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
				List<String> mappingAttributes = new ArrayList<>();
				JSONArray dataRows = jsonData.getJSONArray(entity.getString(Constants.INPUT_ENTITY_NAME));
				for (int j = 0; j < mappings.length(); j++) {
					if (dataRows.getJSONObject(0)
							.has(mappings.getJSONObject(j).getString(Constants.INPUT_ATTRIBUTE_NAME))) {
						columns += mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME) + ", ";
						mappingAttributes.add(mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME));
						columnCount++;
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
				insertResponse = oracleDBMigrator.insertJSONData(metadata, dataRows, mappingAttributes, sql,
						entity.getString(Constants.INPUT_ENTITY_NAME));
			}
		} catch (Exception ex) {
			insertResponse.put(Constants.RESPONSE_STATUS, Constants.RESPONSE_FAILURE);
			insertResponse.put(Constants.RESPONSE_CAUSE, ex.toString());
		}
		return insertResponse;
	}
}
