package metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import utils.Constants;

public class MetadataParser {

	public JSONObject readMetadata(String filePath) throws IOException, FileNotFoundException {
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
			return new JSONObject(out.toString());
		}
		return null;
	}

	public JSONObject parseMetadata(String filePath) throws FileNotFoundException, IOException {
		JSONObject metadata = readMetadata(filePath);
		return parseMetadata(metadata);
	}

	public JSONObject parseMetadata(JSONObject metadata) {
		if (validateMetadata(metadata)) {
			return metadata;
		}
		return null;
	}

	public boolean validateMetadata(JSONObject migratorExt) {
		try {
			JSONObject metadata = migratorExt.getJSONArray(Constants.MIGRATOR_EXT).getJSONObject(0);
			if (!(metadata.has(Constants.INPUT_SOURCE_TYPE) && metadata.has(Constants.INPUT_SOURCE)
					&& metadata.has(Constants.OUTPUT_SOURCE_TYPE) && metadata.has(Constants.OUTPUT_SOURCE))) {
				return false;
			}
			if (!(metadata.has(Constants.SCHEMA) && metadata.getJSONObject(Constants.SCHEMA).has(Constants.INPUT_SCHEMA)
					&& metadata.getJSONObject(Constants.SCHEMA).has(Constants.OUTPUT_SCHEMA)
					&& metadata.getJSONObject(Constants.SCHEMA).has(Constants.ENTITIES))) {
				return false;
			}
			JSONObject schema = metadata.getJSONObject(Constants.SCHEMA);
			if (!(schema.has(Constants.INPUT_ENTITY_NAME) && schema.has(Constants.OUTPUT_ENTITY_NAME)
					&& schema.has(Constants.ENTITIES))) {
				return false;
			}
			JSONArray entities = schema.getJSONArray(Constants.ENTITIES);
			if (entities.isEmpty()) {
				return false;
			}

			for (int i = 0; i < entities.length(); i++) {
				JSONObject entity = entities.getJSONObject(i);
				if (!(entity.has(Constants.OUTPUT_ENTITY_NAME) && entity.has(Constants.INPUT_ENTITY_NAME)
						&& entity.has(Constants.MAPPINGS))) {
					return false;
				}
				JSONArray mappings = entity.getJSONArray(Constants.MAPPINGS);
				if (mappings.isEmpty()) {
					return false;
				}
				for (int j = 0; j < mappings.length(); j++) {
					JSONObject attribute = mappings.getJSONObject(j);
					if (!(attribute.has(Constants.INPUT_ATTRIBUTE_NAME)
							&& attribute.has(Constants.OUTPUT_ATTRIBUTE_NAME))) {

					}
				}
			}
			return true;
		} catch (JSONException ex) {
			return false;
		}
	}
}
