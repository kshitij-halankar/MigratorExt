package converter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.parsers.*;
import org.w3c.dom.*;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import connector.FileExtractor;
import migrator.MongoDBMigrator;
import migrator.OracleDBMigrator;
import utils.Constants;

public class XMLConverter {

	public JSONObject convertXMLToJSON(JSONObject metadata, StringBuilder fileData) {

		// System.out.println("metadata file: " + metadata);
		JSONObject schema = metadata.getJSONObject("Schema");
		JSONArray entities = schema.getJSONArray("Entities");
		// System.out.println("schema: " + schema);
		// System.out.println("entities: " + entities);
		JSONObject xmlTojson = XML.toJSONObject(fileData.toString());
		// System.out.println("json data " + xmlTojson);
		JSONArray menu = new JSONArray();
		for (int i = 0; i < entities.length(); i++) {
			JSONObject entity = entities.getJSONObject(i);
			JSONArray mappings = entity.getJSONArray("Mappings");
			JSONObject test = xmlTojson.getJSONObject(schema.getString(Constants.INPUT_SCHEMA));
			JSONArray testJsonArray = test.getJSONArray(entity.getString(Constants.INPUT_ENTITY_NAME));
			JSONObject food = new JSONObject();
			System.out.println("lengtgh: " + testJsonArray.length());
			// System.out.println("test: "+testJsonArray);
			for (int j = 0; j < testJsonArray.length(); j++) {
				// System.out.println(testJsonArray.getJSONObject(j));
				food = new JSONObject();
				for (int k = 0; k < mappings.length(); k++) {
					if (testJsonArray.getJSONObject(j)
							.has(mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME))) {
						food.put(mappings.getJSONObject(k).getString(Constants.OUTPUT_ATTRIBUTE_NAME),
								testJsonArray.getJSONObject(j)
										.get(mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME)));
					}
				}
				menu.put(food);
			}
		}
		System.out.println("menu: " + menu);
		JSONObject convertedData = new JSONObject();
		convertedData.put("DataArray", menu);
		MongoDBMigrator mongoDBMigrator = new MongoDBMigrator();
		JSONObject response = new JSONObject();
		return response;
	}

	public JSONObject convertAndInsertXMLToSQL(JSONObject metadata) {
		JSONObject response = null;
		String sql = null;
		try {
			String inputFile = metadata.get(Constants.INPUT_SOURCE).toString();
			FileExtractor fileExtractor = new FileExtractor();
			JSONObject jsonData = XML.toJSONObject(fileExtractor.getFile(inputFile).toString());

			System.out.println("jsonData: " + jsonData);

			JSONObject schema = metadata.getJSONObject(Constants.SCHEMA);
			JSONArray entities = schema.getJSONArray(Constants.ENTITIES);
			for (int i = 0; i < entities.length(); i++) {
				JSONObject entity = entities.getJSONObject(i);
				JSONArray mappings = entity.getJSONArray(Constants.MAPPINGS);
				String tableName = entity.getString(Constants.OUTPUT_ENTITY_NAME);
				sql = Constants.SQL_INSERT + tableName;
				sql += "(";
				String columns = "";
				int columnCount = 0;
				List<String> mappingAttributes = new ArrayList<>();
				JSONArray dataRows = jsonData.getJSONArray(entity.getString(Constants.INPUT_ENTITY_NAME));
				for (int j = 0; j < mappings.length(); j++) {
					if (dataRows.getJSONObject(0)
							.has(mappings.getJSONObject(j).getString(Constants.INPUT_ATTRIBUTE_NAME))) {
						columns += mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME) + ", ";
						mappingAttributes.add(mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME));
						columnCount++;
					}
				}
				columns = columns.substring(0, columns.length() - 2);
				String values = "";
				for (int j = 0; j < columnCount; j++) {
					values += "?, ";
				}
				values = values.substring(0, values.length() - 2);
				sql += columns + ")" + Constants.SQL_VALUES + "(" + values + ")";
				System.out.println("sql: " + sql);
				OracleDBMigrator oracleDBMigrator = new OracleDBMigrator();
				oracleDBMigrator.insertJSONData(metadata, dataRows, mappingAttributes, sql,
						entity.getString(Constants.INPUT_ENTITY_NAME));
			}
		} catch (Exception ex) {
			response = new JSONObject();
			response.put(Constants.MIGRATION_STATUS, "failed");
			response.put(Constants.FAILURE_CAUSE, ex.toString());
		}
		return response;
	}

//	public JSONObject convertXMLToSQLAndInsert(JSONObject metadata) {
//		JSONObject response = null;
//		String sql = null;
//		try {
//			String inputFile = metadata.get(Constants.INPUT_SOURCE).toString();
//			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
//			documentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
//			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
//			Document document = documentBuilder.parse(new File(inputFile));
//			document.getDocumentElement().normalize();
//
//			JSONObject schema = metadata.getJSONObject(Constants.SCHEMA);
//			JSONArray entities = schema.getJSONArray(Constants.ENTITIES);
//			for (int i = 0; i < entities.length(); i++) {
//				JSONObject entity = entities.getJSONObject(i);
//				JSONArray mappings = entity.getJSONArray(Constants.MAPPINGS);
//				String tableName = entity.getString(Constants.OUTPUT_ENTITY_NAME);
//				String entityName = entity.getString(Constants.INPUT_ENTITY_NAME);
//
//				NodeList list = document.getElementsByTagName(entityName);
//				for (int j = 0; j < list.getLength(); j++) {
//					Node node = list.item(j);
//					if (node.getNodeType() == Node.ELEMENT_NODE) {
//						Element element = (Element) node;
//						for (int k = 0; k < mappings.length(); k++) {
//							element.getElementsByTagName(
//									mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME)).item(0)
//									.getTextContent();
//						}
//					}
//				}
//			}
//		} catch (Exception ex) {
//			response = new JSONObject();
//			response.put(Constants.MIGRATION_STATUS, "failed");
//			response.put(Constants.FAILURE_CAUSE, ex.toString());
//		}
//		return response;
//	}
}
