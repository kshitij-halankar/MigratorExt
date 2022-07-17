package metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import org.json.JSONException;
import org.json.JSONObject;

import utils.Constants;

public class MetadataParser {
	

	public static JSONObject redMetadata(String filePath) throws IOException, FileNotFoundException {
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

	public static JSONObject parseMetadata(String filePath) throws FileNotFoundException, IOException {
		JSONObject metadata = redMetadata(filePath);
		if (validateMetadata(metadata)) {
			return metadata;
		}
		return null;
	}

	public static boolean validateMetadata(JSONObject metadata) {
		try {
			if (metadata.has(Constants.INPUT_CONNECTION_STRING) && metadata.has(Constants.INPUT_SOURCE)
					&& metadata.has(Constants.OUTPUT_SOURCE) && metadata.has(Constants.INPUT_TYPE)
					&& metadata.has(Constants.OUTPUT_TYPE)) {
				System.out.println("input: " + metadata.toString());
				return true;
			}
		} catch (JSONException ex) {
			return false;
		}
		return false;
	}
}
