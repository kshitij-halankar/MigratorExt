package migrator;

import org.json.JSONObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import connector.MongoDBConnector;
import utils.Constants;
import org.bson.Document;
import org.json.JSONArray;
import java.util.Iterator;
import java.util.List;

public class MongoDBMigrator {

	public int insertData(JSONObject metadata, List<Document> arr) {
		String dbURL = metadata.get(Constants.OUTPUT_SOURCE).toString();
		MongoDBConnector mongoDBConnector = new MongoDBConnector();
		MongoClient client = mongoDBConnector.getMongoClient(dbURL);
		MongoDatabase database = client
				.getDatabase(metadata.getJSONObject(Constants.SCHEMA).getString(Constants.OUTPUT_SCHEMA));
		MongoCollection<Document> collection = database.getCollection(metadata.getJSONObject(Constants.SCHEMA)
				.getJSONArray(Constants.ENTITIES).getJSONObject(0).getString(Constants.OUTPUT_ENTITY_NAME));
		collection.insertMany(arr);
		client.close();
		int insertedRecordsCount = arr.size();
		return insertedRecordsCount;
	}

	public void insertData(JSONObject metadata, JSONArray arr) {
		String dbURL = metadata.get(Constants.OUTPUT_SOURCE).toString();
		MongoDBConnector mongoDBConnector = new MongoDBConnector();
		MongoClient client = mongoDBConnector.getMongoClient(dbURL);
		MongoDatabase database = client
				.getDatabase(metadata.getJSONObject(Constants.SCHEMA).getString(Constants.OUTPUT_SCHEMA));
		MongoCollection<Document> collection = database.getCollection(metadata.getJSONObject(Constants.SCHEMA)
				.getJSONArray(Constants.ENTITIES).getJSONObject(0).getString(Constants.OUTPUT_ENTITY_NAME));
		for (int i = 0; i < arr.length(); i++) {
			Document document = new Document();
			JSONObject temp = arr.getJSONObject(i);
			Iterator iterator = temp.keys();
			while (iterator.hasNext()) {
				Object obj = iterator.next();
				System.out.println(obj + ": " + temp.get(obj.toString()));
				document.append(obj.toString(), temp.get(obj.toString()));
			}
			collection.insertOne(document);
		}
		client.close();
	}
}
