package converter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import connector.OracleDBConnector;
import migrator.OracleDBMigrator;
import utils.Constants;
import java.util.HashMap;

public class CSVConverter {

	public JSONObject convertCSVToJSON(JSONObject metadata) {
		   try {
	            int i, j;
	            HashMap map = new HashMap<>(), attributes = new HashMap();
	            String[] nextRecord;
	            boolean readAttributes = false;
	            JSONArray records = new JSONArray();
	            JSONObject rootObject = metadata;
	            JSONArray metaRecords = rootObject.getJSONArray("MigratorExt").getJSONObject(0).getJSONObject("Schema").getJSONArray("Entities");
	            for (i = 0; i < metaRecords.length(); i++) {
	                JSONArray mappings = metaRecords.getJSONObject(i).getJSONArray("Mappings");
	                for ( j = 0; j < mappings.length(); j++)
	                    map.put(mappings.getJSONObject(j).getString("InputAttributeName"),mappings.getJSONObject(j).getString("OutputAttributeName"));

	                FileReader filereader = new FileReader(rootObject.getJSONArray("MigratorExt").getJSONObject(0).getString("InputSource"));
	                CSVReader csvReader = new CSVReader(filereader);
	                while ((nextRecord = csvReader.readNext()) != null) {
	                    if (!readAttributes) {
	                        j=0;
	                        for (String cell : nextRecord){
	                            if(map.containsKey(cell))
	                                attributes.put(j,map.get(cell));
	                            j++;
	                        }
	                        readAttributes = true;
	                    } else {
	                        JSONObject tempObject = new JSONObject();
	                        j=0;
	                        for (String cell : nextRecord) {
	                            if(attributes.containsKey(j))
	                                tempObject.put(attributes.get(j).toString(), cell);
	                            j++;
	                        }
	                        records.put(tempObject);
	                    }
	                }
	            }

	            writeToFile(targetFileName, records.toString());

	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (CsvValidationException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
		
		
		
		
		
		
//		JSONObject result = null;
//		int i;
//		String[] nextRecord;
//		boolean readAttributes = false;
//		JSONArray records = new JSONArray();
//		ArrayList attributes = new ArrayList();
//
//
//		try {
//			FileReader filereader = new FileReader(fileData.toString());
//			CSVReader csvReader = new CSVReader(filereader);
//			while ((nextRecord = csvReader.readNext()) != null) {
//				if (!readAttributes) {
//					for (String cell : nextRecord)
//						attributes.add(cell);
//					readAttributes = true;
//				} else {
//					JSONObject tempObject = new JSONObject();
//					i = 0;
//					for (String cell : nextRecord) {
//						tempObject.put(attributes.get(i).toString(), cell);
//						i++;
//					}
//					records.put(tempObject);
//				}
//
//			}
//			result.put("convertedValue", records);
//
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (CsvValidationException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		return result;
	}

	public JSONObject convertCSVToSQLAndInsert(JSONObject metadata) {
		JSONObject response = null;
		String sql = null;
		try {
//			System.out.println(metadata);
			String inputFile = metadata.get(Constants.INPUT_SOURCE).toString();
//			System.out.println("inputFile: " + inputFile);
//			BufferedReader lineReader = new BufferedReader(new FileReader(inputFile));
			CSVReader csvReader = new CSVReader(new FileReader(inputFile));
			String lineText = null;
			int count = 0;
//			lineText = lineReader.readLine();
//			System.out.println(lineText);
			String headers[] = csvReader.readNext();
//			System.out.println(headers.length);
//			for(int k=0;k<headers.length;k++) {
//				System.out.println(headers[k]);
//			}

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
				List<Integer> columnNumber = new ArrayList<>();
				for (String attribute : headers) {
//					System.out.println(attribute);
					for (int j = 0; j < mappings.length(); j++) {
						if (mappings.getJSONObject(j).getString(Constants.INPUT_ATTRIBUTE_NAME).equals(attribute)) {
							columns += mappings.getJSONObject(j).getString(Constants.OUTPUT_ATTRIBUTE_NAME) + ", ";
							columnCount++;
							columnNumber.add(j);
						}
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
				oracleDBMigrator.insertCSVData(entity, csvReader, sql, columnNumber);

			}
//			lineReader.close();
			csvReader.close();

		} catch (Exception ex) {
			response = new JSONObject();
			response.put(Constants.MIGRATION_STATUS, "failed");
			response.put(Constants.FAILURE_CAUSE, ex.toString());
		}
		return response;
	}
}
