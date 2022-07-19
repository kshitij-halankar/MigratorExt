package converter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.json.JSONObject;

import utils.Constants;

public class XMLConverter {

	public JSONObject convertXMLToJSON(JSONObject metadata, StringBuilder fileData) {
		
		Strind xmlData = readMetadata(fileData);
		JSONObject xmlTojson = XML.toJSONObject(xmlData);
		System.out.println("json data"+xmlTojson);
		
		//open metadata file
		String input = "D:\\temp\\src\\temp\\xml_mongo.json";
		//JSONObject metadata = readMetadata(input);
	    System.out.println("metadata file: "+metadata);
	    
	    JSONObject schema = metadata.getJSONArray("MigratorExt").getJSONObject(0).getJSONObject("Schema");
	    JSONArray entities = schema.getJSONArray("Entities");
		
		System.out.println("schema: "+schema);
		System.out.println("entities: "+entities);
		
		JSONArray menu = new JSONArray();
		for (int i = 0; i < entities.length(); i++) {
			JSONObject entity = entities.getJSONObject(i);
			JSONArray mappings = entity.getJSONArray("Mappings");
			//might change food later while testing
			JSONArray test = xmlTojson.getJSONObject(metadata.get(Constants.INPUT_ENTITY_NAME)).getJSONArray("food");
			JSONObject food = new JSONObject();
			for (int j = 0; j < mappings.length(); j++) {
				
				if (test.getJSONObject(j).has(mappings.getJSONObject(j).getString("InputAttributeName"))) {
					food.put(mappings.getJSONObject(j).getString("OutputAttributeName"), 
							test.getJSONObject(j).get(mappings.getJSONObject(j).getString("InputAttributeName")));
				}
				menu.put(food);
			}	
		}
		//JSONObject menu = null;
		return menu;
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
	
	public String readMetadata(String filePath) throws IOException, FileNotFoundException {
		File f = new File(filePath);
		if (f.exists()) {
			InputStream is = new FileInputStream(filePath);
			char[] buf = new char[1024];
			int buflen = buf.length;
			StringBuilder out = new StringBuilder();
			Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
			for (int i; (i = reader.read(buf, 0, buflen)) > 0;) {
				out.append(buf, 0, i);
			}
			reader.close();
			return (out.toString());
		}
		return null;
	}
}
