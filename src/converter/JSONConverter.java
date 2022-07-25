package converter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;

import connector.OracleDBConnector;
import metadata.MetadataParser;
import migrator.MongoDBMigrator;
import migrator.OracleDBMigrator;
import utils.Constants;

public class JSONConverter {

	public JSONObject insertJSONToMongo(JSONObject metadata, JSONObject fileData) {
		JSONObject result = null;
		int batchSize = 0;
		JSONObject schema = metadata.getJSONObject(Constants.SCHEMA);
		JSONArray entities = schema.getJSONArray(Constants.ENTITIES);
		for (int i = 0; i < entities.length(); i++) {
			JSONObject entity = entities.getJSONObject(i);
			JSONArray mappings = entity.getJSONArray(Constants.MAPPINGS);
			String collectionName = entity.getString(Constants.OUTPUT_ENTITY_NAME);
//			JSONArray records = new JSONArray();
			List<Document> records = new ArrayList<>();
			JSONObject mappingAttributes = new JSONObject();
			for (int k = 0; k < mappings.length(); k++) {
				mappingAttributes.put(mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME),
						mappings.getJSONObject(k).getString(Constants.OUTPUT_ATTRIBUTE_NAME));
			}
			System.out.println(mappingAttributes);
//            System.out.println(fileData.toString());
			JSONArray fileDataArray = fileData.getJSONArray(schema.getString(Constants.INPUT_SCHEMA));
			for (int j = 0; j < fileDataArray.length(); j++) {
//				JSONObject data = new JSONObject();
				Document tempObject = new Document();
				JSONObject dataRow = fileDataArray.getJSONObject(j);
				Iterator<String> dataRowKeys = mappingAttributes.keys();
				while (dataRowKeys.hasNext()) {
					String key = dataRowKeys.next();
					tempObject.append(mappingAttributes.getString(key), dataRow.getString(key));
//					data.put(mappingAttributes.getString(key), dataRow.getString(key));

				}
				records.add(tempObject);
				batchSize++;
				if (batchSize == Constants.BATCH_SIZE) {
					MongoDBMigrator mongoMigrator = new MongoDBMigrator();
					mongoMigrator.insertData(metadata, records);
					batchSize = 0;
					records = new ArrayList<>();
				}
			}
//			System.out.println(records.toString());
			if (batchSize > 0) {
				MongoDBMigrator mongoMigrator = new MongoDBMigrator();
				mongoMigrator.insertData(metadata, records);
			}
		}
		return result;
	}

	public JSONObject fetchJSONFromMongoAndInsertToSQL(JSONObject metadata) {
		JSONObject response = null;
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
		        MongoCollection<Document> collection = database.getCollection(entity.getString(Constants.INPUT_ENTITY_NAME));
		        List<String> includeFields=new ArrayList<>();
		        List<String> mappingAttributes = new ArrayList<>();
				int columnCount = 0;
				String columns = "";
		        for(int j=0; j<mappings.length();j++) {
		        	includeFields.add(mappings.getJSONObject(j).getString(Constants.INPUT_ATTRIBUTE_NAME));
		        	mappingAttributes.add(mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME));
		        	columns += mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME) + ", ";
					columnCount++;
		        }
		        Bson projection = Projections.fields(Projections.include(includeFields), Projections.excludeId());
		        System.out.println("projection: "+projection);
		        
	            FindIterable<Document> iterable = collection.find().projection(projection);
	            Iterator iterator = iterable.iterator();
	            JSONArray dataRows = new JSONArray();
	            while (iterator.hasNext()) {
	            	JSONObject dataRow=new JSONObject(((Document) iterator.next()).toJson());
	            	dataRows.put(dataRow);
	                System.out.println(dataRow);
	            	
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
				System.out.println("sql: " + sql);
				OracleDBMigrator oracleDBMigrator = new OracleDBMigrator();
				oracleDBMigrator.insertJSONData(metadata, dataRows, mappingAttributes, sql,
						entity.getString(Constants.INPUT_ENTITY_NAME));
			}
		} catch (Exception ex) {
			response = new JSONObject();
			response.put(Constants.MIGRATION_STATUS, "failed");
			response.put(Constants.FAILURE_CAUSE, ex.toString());
		}
		return response;
	}

	public JSONObject convertJSONToSQLAndInsert(JSONObject metadata) {
		JSONObject response = null;
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
//                JSONArray dataRows = jsonData.getJSONArray(entity.getString(Constants.INPUT_ENTITY_NAME));
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
				System.out.println("sql: " + sql);
				OracleDBMigrator oracleDBMigrator = new OracleDBMigrator();
				oracleDBMigrator.insertJSONData(metadata, dataRows, mappingAttributes, sql,
						entity.getString(Constants.INPUT_ENTITY_NAME));
			}
		} catch (Exception ex) {
			response = new JSONObject();
			response.put(Constants.MIGRATION_STATUS, "failed");
			response.put(Constants.FAILURE_CAUSE, ex.toString());
		}
		return response;
	}
}