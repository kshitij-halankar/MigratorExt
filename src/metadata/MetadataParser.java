package metadata;

import org.json.JSONException;
import org.json.JSONObject;

import utils.Constants;

public class MetadataParser {

	public static void redMetadata() {

	}

	public static boolean parseMetadata() {
		JSONObject metadata = new JSONObject();
		if (validateMetadata(metadata)) {
			
		}
		return false;
	}

	public static boolean validateMetadata(JSONObject metadata) {
		try {
			if (metadata.has(Constants.CONNECTION_STRING) && 
					metadata.has(Constants.INPUT_SOURCE) &&
					metadata.has(Constants.OUTPUT_SOURCE) &&
					metadata.has(Constants.INPUT_TYPE) &&
					metadata.has(Constants.OUTPUT_TYPE)) {
				return true;
			}
		} catch (JSONException ex) {
			return false;
		}
		
		return false;
	}

	public static void setMetadataProperties() {

	}
}
