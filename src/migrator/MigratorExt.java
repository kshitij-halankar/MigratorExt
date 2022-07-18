package migrator;

import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import metadata.DBPropertiesObj;
import metadata.MetadataParser;
import migrator.MongoDBMigrator;
public class MigratorExt {

	public static void main(String[] args) {
		String input = "C:\\Users\\kshit\\Documents\\MAC\\ADT\\Project\\v1\\MigratorExt\\src\\test\\input.json";
		try {
			JSONObject metadata = MetadataParser.parseMetadata(input);
			DBPropertiesObj dbp = new DBPropertiesObj(metadata);

	    
		} catch (Exception ex) {
			System.out.println(ex.toString());
		}
	}
}