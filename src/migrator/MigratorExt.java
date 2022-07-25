package migrator;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import connector.FileExtractor;
import converter.CSVConverter;
import converter.JSONConverter;
import converter.ResultSetConverter;
import converter.XMLConverter;
import metadata.DBPropertiesObj;
import metadata.MetadataParser;
import utils.Constants;

public class MigratorExt {

	public static void main(String[] args) {
//        String input = "C:\\Users\\kshit\\Documents\\MAC\\ADT\\Project\\v1\\MigratorExt\\src\\test\\input.json";
//        String input = "src\\test\\csv_mongo.json";
//        String input = "src\\test\\json_mongo.json";
//        System.out.println(System.getProperty("user.dir"));
//        String input = "src\\test\\csv_oracle_sample1.json";
//        String input = "src\\test\\json_oracle_sample.json";
//    	String input="src\\test\\xml_oracle_sample.json";
//		String input = "src\\test\\oracle_mongo_sample.json";
		String input="src\\test\\mongo_oracle_sample.json";
		try {
//            MetadataParser metadataParser = new MetadataParser();
//            JSONObject metadata = metadataParser.parseMetadata(input);
//            DBPropertiesObj dbp = new DBPropertiesObj(metadata);
			MigratorExt me = new MigratorExt();
			System.out.println(me.migrate(input));
		} catch (Exception ex) {
			System.out.println(ex.toString());
		}
	}

	public JSONObject migrate(String metadataSource) throws FileNotFoundException, IOException {
		MetadataParser metadataParser = new MetadataParser();
		return migrate(metadataParser.parseMetadata(metadataSource));
	}

	public JSONObject migrate(JSONObject metadata) {
		JSONObject response = null;
		try {
			// parse & validate metadata
			MetadataParser metadataParser = new MetadataParser();
//            if (!metadataParser.validateMetadata(metadata)) {
//                System.out.println("hallo");
//                throw new JSONException("Error in Input Metadata.");
//            }
			JSONArray migratorExt = metadata.getJSONArray(Constants.MIGRATOR_EXT);
			for (int i = 0; i < migratorExt.length(); i++) {
				JSONObject metadataObj = migratorExt.getJSONObject(i);
				FileExtractor fileExtractor = new FileExtractor();
				JSONArray extractedData = null;

				if (metadataObj.getString(Constants.OUTPUT_SOURCE_TYPE).toString().equals(Constants.ORACLE)) {
					// connect to input source & extract data
					switch (metadataObj.get(Constants.INPUT_SOURCE_TYPE).toString()) {
					case Constants.MONGO:
						JSONConverter jsonConverter = new JSONConverter();
						response = jsonConverter.fetchJSONFromMongoAndInsertToSQL(metadataObj);
						break;
					case Constants.CSV:
						CSVConverter csvConverter = new CSVConverter();
						response = csvConverter.convertCSVToSQLAndInsert(metadataObj);
						break;
					case Constants.JSON:
						JSONConverter jsonConvert = new JSONConverter();
						response = jsonConvert.convertJSONToSQLAndInsert(metadataObj);
						break;
					case Constants.XML:
						XMLConverter xmlConverter = new XMLConverter();
//                        response = xmlConverter.convertXMLToSQLAndInsert(metadataObj);
						response = xmlConverter.convertAndInsertXMLToSQL(metadataObj);
						break;
					}

				} else if (metadataObj.getString(Constants.OUTPUT_SOURCE_TYPE).toString().equals(Constants.MONGO)) {

					// connect to input source & extract data
					switch (metadataObj.get(Constants.INPUT_SOURCE_TYPE).toString()) {
					case Constants.ORACLE:
						ResultSetConverter resultSetConverter = new ResultSetConverter();
						response = resultSetConverter.convertResultSetToJSON(metadataObj);
						break;
					case Constants.CSV:
						CSVConverter csvConverter = new CSVConverter();
						response = csvConverter.convertCSVToJSON(metadataObj);
						break;
					case Constants.JSON:
						response = fileExtractor.extractJSONAndConvertForMongo(metadataObj);
						break;
					case Constants.XML:
						response = fileExtractor.extractXMLAndConvertForMongo(metadataObj);
						break;
					}
				}

			}

			// connect to output source & insert data - inside convertor class

		} catch (Exception ex) {
			response = new JSONObject();
			response.put(Constants.MIGRATION_STATUS, "failed");
			response.put(Constants.FAILURE_CAUSE, ex.toString());
		}
		return response;
	}
}