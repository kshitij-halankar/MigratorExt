package migrator;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import connector.FileExtractor;
import metadata.DBPropertiesObj;
import metadata.MetadataParser;
import utils.Constants;

public class MigratorExt {

	public static void main(String[] args) {
		String input = "C:\\Users\\kshit\\Documents\\MAC\\ADT\\Project\\v1\\MigratorExt\\src\\test\\input.json";
		try {
			MetadataParser metadataParser = new MetadataParser();
			JSONObject metadata = metadataParser.parseMetadata(input);
//			DBPropertiesObj dbp = new DBPropertiesObj(metadata);
		} catch (Exception ex) {
			System.out.println(ex.toString());
		}
	}

	public JSONObject migrate(String metadataSource) throws FileNotFoundException, IOException {
		MetadataParser metadataParser = new MetadataParser();
		return migrate(metadataParser.parseMetadata(metadataSource));
	}

	public JSONObject migrate(JSONObject metadata) {
		JSONObject response = new JSONObject();
		try {
			// parse & validate metadata
			MetadataParser metadataParser = new MetadataParser();
			if (!metadataParser.validateMetadata(metadata)) {
				throw new JSONException("Error in Input Metadata.");
			}
			JSONArray migratorExt = metadata.getJSONArray(Constants.MIGRATOR_EXT);
			for (int i = 0; i < migratorExt.length(); i++) {
				JSONObject metadataObj = migratorExt.getJSONObject(i);
				FileExtractor fileExtractor = new FileExtractor();
				JSONObject extractedData = null;

				// connect to input source & extract data
				switch (metadataObj.get(Constants.INPUT_SOURCE_TYPE).toString()) {
				case Constants.MONGO:
					break;
				case Constants.ORACLE:
					break;
				case Constants.CSV:
					extractedData = fileExtractor.extractCSVAndConvert(metadataObj);
					break;
				case Constants.JSON:
					extractedData = fileExtractor.extractJSONAndConvert(metadataObj);
					break;
				case Constants.XML:
					extractedData = fileExtractor.extractXMLAndConvert(metadataObj);
					break;
				}

				if(metadataObj.getString(Constants.OUTPUT_SOURCE_TYPE).toString().equals(Constants.ORACLE)) {
					OracleDBMigrator oracleDBMigrator = new OracleDBMigrator();
					oracleDBMigrator.insertData(metadataObj,extractedData);
				} else if(metadataObj.getString(Constants.OUTPUT_SOURCE_TYPE).toString().equals(Constants.MONGO)) {
					MongoDBMigrator mongoDBMigrator = new MongoDBMigrator();
					mongoDBMigrator.insertData(metadataObj,extractedData);
				}
				
			}

			// connect to output source & insert data - inside convertor class

		} catch (Exception ex) {
			response.put(Constants.MIGRATION_STATUS, "failed");
			response.put(Constants.FAILURE_CAUSE, ex.toString());
		}
		return response;
	}
}