package converter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import metadata.MetadataParser;
import migrator.MongoDBMigrator;
import migrator.OracleDBMigrator;
import utils.Constants;

public class JSONConverter {

    public JSONObject insertJSONToMongo(JSONObject metadata, JSONObject fileData) {
        JSONObject result = null;
        JSONObject schema = metadata.getJSONObject(Constants.SCHEMA);
        JSONArray entities = schema.getJSONArray(Constants.ENTITIES);
        for (int i = 0; i < entities.length(); i++) {
            JSONObject entity = entities.getJSONObject(i);
            JSONArray mappings = entity.getJSONArray(Constants.MAPPINGS);
            String collectionName = entity.getString(Constants.OUTPUT_ENTITY_NAME);
            JSONArray records = new JSONArray();
            JSONObject mappingAttributes = new JSONObject();
            for (int k = 0; k < mappings.length(); k++) {
                mappingAttributes.put(mappings.getJSONObject(k).getString(Constants.INPUT_ATTRIBUTE_NAME),
                        mappings.getJSONObject(k).getString(Constants.OUTPUT_ATTRIBUTE_NAME));
            }
            System.out.println(mappingAttributes);
//            System.out.println(fileData.toString());
            JSONArray fileDataArray = fileData.getJSONArray(schema.getString(Constants.INPUT_SCHEMA));
            for (int j = 0; j < fileDataArray.length(); j++) {
                JSONObject data = new JSONObject();

                JSONObject dataRow = fileDataArray.getJSONObject(j);
                Iterator<String> dataRowKeys = mappingAttributes.keys();
                while (dataRowKeys.hasNext()) {
                    String key = dataRowKeys.next();
                    data.put(mappingAttributes.getString(key), dataRow.getString(key));
                }
                records.put(data);
            }
            System.out.println(records.toString());
            MongoDBMigrator mongoMigrator = new MongoDBMigrator();
            mongoMigrator.insertData(metadata, records);
        }
        return result;
    }

    public JSONObject convertJSONToSQLAndInsert(JSONObject metadata) {
        JSONObject response = null;
        String sql = null;
        try {
            String inputFile = metadata.get(Constants.INPUT_SOURCE).toString();
            MetadataParser metadataParser = new MetadataParser();
            JSONObject jsonData = metadataParser.readMetadata(inputFile);
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
//                JSONArray dataRows = jsonData.getJSONArray(entity.getString(Constants.INPUT_ENTITY_NAME));
                JSONArray dataRows = jsonData.getJSONArray(schema.getString(Constants.INPUT_SCHEMA));
                List<String> mappingAttributes = new ArrayList<>();
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
}