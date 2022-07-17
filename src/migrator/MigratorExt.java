package migrator;

import org.json.JSONObject;

import metadata.DBPropertiesObj;
import metadata.MetadataParser;

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