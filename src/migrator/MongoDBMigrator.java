package migrator;

import org.json.JSONObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.Iterator;

public class MongoDBMigrator {

	public void insertData(JSONObject metadata, JSONObject data) {
		JSONArray arr = new JSONArray();
        for (int i = 0; i < 10; i++) {
            JSONObject object = new JSONObject();
            object.put("key1" + i, i);
            object.put("key2" + i, 1.0 + i);
            object.put("key3" + i, "lol" + i);
            arr.put(object);
        }
        
		MongoClient client = MongoClients.create("mongodb://localhost:27017");
		MongoDatabase database = client.getDatabase("SampleData");
        MongoCollection<Document> collection = database.getCollection("SampleCollection");

        for (int i = 0; i < arr.length(); i++) {
            Document document = new Document();
            JSONObject temp = arr.getJSONObject(i);
            Iterator iterator = temp.keys();
            while (iterator.hasNext()) {
                Object obj = iterator.next();
                System.out.println(obj + ": " + temp.get(obj.toString()));
                document.append(obj.toString(), temp.get(obj.toString())); // you add this into the collection using collection.insert(basicDBObject)
            }
            collection.insertOne(document);
        }
        client.close();
	}
}
