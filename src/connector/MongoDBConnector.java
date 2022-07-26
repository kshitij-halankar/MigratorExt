package connector;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoDBConnector {

	public MongoClient getMongoClient(String dbURL) {
		MongoClient client = MongoClients.create(dbURL);
		return client;
	}
}
