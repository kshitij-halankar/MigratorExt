package converter;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import com.opencsv.CSVReader;
import migrator.MongoDBMigrator;
import migrator.OracleDBMigrator;
import utils.Constants;
import java.util.HashMap;

public class CSVConverter {

	public JSONObject convertCSVToJSON(JSONObject metadata) {
		JSONObject insertResponse = new JSONObject();
		int insertedRecordsCount = 0;
		try {
			System.out.println(metadata);
			int i, j, batchSize = 0;
			HashMap map = new HashMap<>(), attributes = new HashMap();
			String[] nextRecord;
			boolean readAttributes = false;
			List<Document> records = new ArrayList<>();
			JSONObject rootObject = metadata;
			JSONArray metaRecords = rootObject.getJSONObject(Constants.SCHEMA).getJSONArray(Constants.ENTITIES);
			for (i = 0; i < metaRecords.length(); i++) {
				JSONArray mappings = metaRecords.getJSONObject(i).getJSONArray(Constants.MAPPINGS);
				for (j = 0; j < mappings.length(); j++)
					map.put(mappings.getJSONObject(j).getString(Constants.INPUT_ATTRIBUTE_NAME),
							mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME));

				FileReader filereader = new FileReader(rootObject.getString(Constants.INPUT_SOURCE));
				CSVReader csvReader = new CSVReader(filereader);
				while ((nextRecord = csvReader.readNext()) != null) {
					if (!readAttributes) {
						j = 0;
						for (String cell : nextRecord) {
							if (map.containsKey(cell))
								attributes.put(j, map.get(cell));
							j++;
						}
						readAttributes = true;
					} else {
						Document tempObject = new Document();
						j = 0;
						for (String cell : nextRecord) {
							if (attributes.containsKey(j))
								tempObject.append(attributes.get(j).toString(), cell);
							j++;
						}
						records.add(tempObject);
						batchSize++;
						if (batchSize == Constants.BATCH_SIZE) {
							MongoDBMigrator mongoMigrator = new MongoDBMigrator();
							insertedRecordsCount += mongoMigrator.insertData(metadata, records);
							batchSize = 0;
							records = new ArrayList<>();
						}
					}
				}
			}

			if (batchSize > 0) {
				MongoDBMigrator mongoMigrator = new MongoDBMigrator();
				insertedRecordsCount += mongoMigrator.insertData(metadata, records);
			}
			insertResponse.put(Constants.RESPONSE_STATUS, Constants.RESPONSE_SUCCESS);
			insertResponse.put(Constants.RESPONSE_TOTAL_RECORDS_INSERTED, insertedRecordsCount);
		} catch (Exception ex) {
			insertResponse.put(Constants.RESPONSE_STATUS, Constants.RESPONSE_FAILURE);
			insertResponse.put(Constants.RESPONSE_CAUSE, ex.toString());
		}
		return insertResponse;
	}

	public JSONObject convertCSVToSQLAndInsert(JSONObject metadata) {
		JSONObject insertResponse = new JSONObject();
		String sql = null;
		try {
			String inputFile = metadata.get(Constants.INPUT_SOURCE).toString();
			CSVReader csvReader = new CSVReader(new FileReader(inputFile));
			String lineText = null;
			int count = 0;
			String headers[] = csvReader.readNext();
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
				List<Integer> columnNumber = new ArrayList<>();
				for (String attribute : headers) {
					for (int j = 0; j < mappings.length(); j++) {
						if (mappings.getJSONObject(j).getString(Constants.INPUT_ATTRIBUTE_NAME).equals(attribute)) {
							columns += mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME) + ", ";
							columnCount++;
							columnNumber.add(j);
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
				insertResponse = oracleDBMigrator.insertCSVData(metadata, entity, csvReader, sql, columnNumber,
						tableName);
			}
			csvReader.close();

		} catch (Exception ex) {
			insertResponse.put(Constants.RESPONSE_STATUS, Constants.RESPONSE_FAILURE);
			insertResponse.put(Constants.RESPONSE_CAUSE, ex.toString());
		}
		return insertResponse;
	}
}
