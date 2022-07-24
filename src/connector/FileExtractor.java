package connector;

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
import converter.CSVConverter;
import converter.JSONConverter;
import converter.XMLConverter;
import utils.Constants;

public class FileExtractor {
//	public JSONObject extractCSVAndConvertForMongo(JSONObject metadata) throws IOException, FileNotFoundException {
//		String filePath = metadata.get(Constants.INPUT_SOURCE).toString();
//		StringBuilder fileData = getFile(filePath);
//		CSVConverter csvConverter = new CSVConverter();
//		return csvConverter.convertCSVToJSON(metadata, fileData);
//	}

	public JSONArray extractXMLAndConvertForMongo(JSONObject metadata) throws IOException, FileNotFoundException {
		String filePath = metadata.get(Constants.INPUT_SOURCE).toString();
		StringBuilder fileData = getFile(filePath);
		XMLConverter xmlConverter = new XMLConverter();
		return xmlConverter.convertXMLToJSON(metadata, fileData);
	}

	public JSONArray extractJSONAndConvertForMongo(JSONObject metadata) throws IOException, FileNotFoundException {
		String filePath = metadata.get(Constants.INPUT_SOURCE).toString();
		StringBuilder fileData = getFile(filePath);
		JSONConverter jsonConverter = new JSONConverter();
		return jsonConverter.convertJSONToBSON(metadata, fileData);
	}

	public StringBuilder getFile(String filePath) throws IOException {
		File f = new File(filePath);
		if (f.exists()) {
			InputStream is = new FileInputStream(filePath);
			char[] buffer = new char[1024];
			int len = buffer.length;
			StringBuilder out = new StringBuilder();
			Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
			for (int i; (i = reader.read(buffer, 0, len)) > 0;) {
				out.append(buffer, 0, i);
			}
			reader.close();
			return out;
		}
		return null;
	}
}
