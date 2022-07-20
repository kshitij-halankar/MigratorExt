package converter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;

import migrator.MongoDBMigrator;
import utils.Constants;

public class XMLConverter {

	public JSONObject convertXMLToJSON(JSONObject metadata, StringBuilder fileData) {

		JSONObject xmlTojson = XML.toJSONObject(fileData.toString());
		System.out.println("json data" + xmlTojson);

		// open metadata file
		String input = "D:\\temp\\src\\temp\\xml_mongo.json";
		// JSONObject metadata = readMetadata(input);
		System.out.println("metadata file: " + metadata);

		JSONObject schema = metadata.getJSONArray("MigratorExt").getJSONObject(0).getJSONObject("Schema");
		JSONArray entities = schema.getJSONArray("Entities");

		System.out.println("schema: " + schema);
		System.out.println("entities: " + entities);

		JSONArray menu = new JSONArray();
		for (int i = 0; i < entities.length(); i++) {
			JSONObject entity = entities.getJSONObject(i);
			JSONArray mappings = entity.getJSONArray("Mappings");
			// might change food later while testing
			JSONArray test = xmlTojson.getJSONArray(metadata.getString(Constants.INPUT_ENTITY_NAME));
			JSONObject food = new JSONObject();
			for (int j = 0; j < mappings.length(); j++) {

				if (test.getJSONObject(j).has(mappings.getJSONObject(j).getString("InputAttributeName"))) {
					food.put(mappings.getJSONObject(j).getString("OutputAttributeName"),
							test.getJSONObject(j).get(mappings.getJSONObject(j).getString("InputAttributeName")));
				}
				menu.put(food);
			}
		}
		JSONObject convertedData=new JSONObject();
		convertedData.put("DataArray", menu);
		MongoDBMigrator mongoDBMigrator = new MongoDBMigrator();
		mongoDBMigrator.insertData(metadata, convertedData);
		// JSONObject menu = null;
		JSONObject response = new JSONObject();
		return response;
	}

	public JSONObject convertXMLToSQLAndInsert(JSONObject metadata) {
		JSONObject response = null;
		String sql = null;
		try {

		} catch (Exception ex) {
			response = new JSONObject();
			response.put(Constants.MIGRATION_STATUS, "failed");
			response.put(Constants.FAILURE_CAUSE, ex.toString());
		}
		return response;
	}
}
