package converter;

import org.json.JSONObject;

import utils.Constants;

public class JSONConverter {

	public JSONObject convertJSONToBSON(JSONObject metadata, StringBuilder fileData) {
		JSONObject result = null;
		return result;
	}

	public JSONObject convertJSONToSQLAndInsert(JSONObject metadata) {
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
