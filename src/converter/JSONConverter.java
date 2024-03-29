package converter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import metadata.MetadataParser;
import migrator.MongoDBMigrator;
import migrator.OracleDBMigrator;
import utils.Constants;

public class JSONConverter {

	public JSONObject insertJSONToMongo(JSONObject metadata, JSONObject fileData) {
		JSONObject insertResponse = new JSONObject();
		int insertedRecordsCount = 0;
		int batchSize = 0;
		JSONObject schema = metadata.getJSONObject(Constants.SCHEMA);
		JSONArray entities = schema.getJSONArray(Constants.ENTITIES);
		for (int i = 0; i < entities.length(); i++) {
			JSONObject entity = entities.getJSONObject(i);
			JSONArray mappings = entity.getJSONArray(Constants.MAPPINGS);
			String collectionName = entity.getString(Constants.OUTPUT_ENTITY_NAME);
			List<Document> records = new ArrayList<>();
			JSONObject mappingAttributes = new JSONObject();
			for (int k = 0; k < mappings.length(); k++) {
				mappingAttributes.put(mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME),
						mappings.getJSONObject(k).getString(Constants.OUTPUT_ATTRIBUTE_NAME));
			}
			System.out.println(mappingAttributes);
			JSONArray fileDataArray = fileData.getJSONArray(schema.getString(Constants.INPUT_SCHEMA));
			for (int j = 0; j < fileDataArray.length(); j++) {
				Document tempObject = new Document();
				JSONObject dataRow = fileDataArray.getJSONObject(j);
				Iterator<String> dataRowKeys = mappingAttributes.keys();
				while (dataRowKeys.hasNext()) {
					String key = dataRowKeys.next();
					tempObject.append(mappingAttributes.getString(key), dataRow.getString(key));
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
			if (batchSize > 0) {
				MongoDBMigrator mongoMigrator = new MongoDBMigrator();
				insertedRecordsCount += mongoMigrator.insertData(metadata, records);
			}
		}
		insertResponse.put(Constants.RESPONSE_STATUS, Constants.RESPONSE_SUCCESS);
		insertResponse.put(Constants.RESPONSE_TOTAL_RECORDS_INSERTED, insertedRecordsCount);
		return insertResponse;
	}

	public JSONObject fetchJSONFromMongoAndInsertToSQL(JSONObject metadata) {
		JSONObject insertResponse = new JSONObject();
		try {
			String dbURL = metadata.get(Constants.INPUT_SOURCE).toString();
			String dbUserName = metadata.get(Constants.INPUT_SOURCE_LOGIN_USERNAME).toString();
			String dbPassword = metadata.get(Constants.INPUT_SOURCE_LOGIN_PASSWORD).toString();
			JSONObject schema = metadata.getJSONObject(Constants.SCHEMA);
			JSONArray entities = schema.getJSONArray(Constants.ENTITIES);
			for (int i = 0; i < entities.length(); i++) {
				JSONObject entity = entities.getJSONObject(i);
				JSONArray mappings = entity.getJSONArray(Constants.MAPPINGS);
				MongoClient client = MongoClients.create(dbURL);
				MongoDatabase database = client.getDatabase(schema.getString(Constants.INPUT_SCHEMA));
				MongoCollection<Document> collection = database
						.getCollection(entity.getString(Constants.INPUT_ENTITY_NAME));
				List<String> includeFields = new ArrayList<>();
				List<String> mappingAttributes = new ArrayList<>();
				int columnCount = 0;
				String columns = "";
				for (int j = 0; j < mappings.length(); j++) {
					includeFields.add(mappings.getJSONObject(j).getString(Constants.INPUT_ATTRIBUTE_NAME));
					mappingAttributes.add(mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME));
					columns += mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME) + ", ";
					columnCount++;
				}
				Bson projection = Projections.fields(Projections.include(includeFields), Projections.excludeId());

				FindIterable<Document> iterable = collection.find().projection(projection);
				Iterator iterator = iterable.iterator();
				JSONArray dataRows = new JSONArray();
				while (iterator.hasNext()) {
					JSONObject dataRow = new JSONObject(((Document) iterator.next()).toJson());
					dataRows.put(dataRow);

				}
				String tableName = entity.getString(Constants.OUTPUT_ENTITY_NAME);
				String sql = Constants.SQL_INSERT + tableName;
				sql += "(";
				columns = columns.substring(0, columns.length() - 2);
				String values = "";
				for (int j = 0; j < columnCount; j++) {
					values += "?, ";
				}
				values = values.substring(0, values.length() - 2);
				sql += columns + ")" + Constants.SQL_VALUES + "(" + values + ")";
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

	public JSONObject convertJSONToSQLAndInsert(JSONObject metadata) {
		JSONObject insertResponse = new JSONObject();
		String sql = null;
		try {
			String inputFile = metadata.get(Constants.INPUT_SOURCE).toString();
			MetadataParser metadataParser = new MetadataParser();
			JSONObject jsonData = metadataParser.readMetadata(inputFile);
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
				JSONArray dataRows = jsonData.getJSONArray(schema.getString(Constants.INPUT_SCHEMA));
				List<String> mappingAttributes = new ArrayList<>();
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