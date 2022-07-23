package migrator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.json.JSONObject;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import connector.OracleDBConnector;
import utils.Constants;

public class OracleDBMigrator {

	public int[] insertCSVData(JSONObject metadata, CSVReader csvReader, String sql)
			throws SQLException, CsvValidationException, IOException {
		// convert CSV to SQL
		String[] row = null;
		OracleDBConnector oracleDBConnector = new OracleDBConnector();
		Connection conn = oracleDBConnector.getConnection();
		PreparedStatement p = conn.prepareStatement(sql);
		int batchSize = 0;
		int[] insertResult = null;
		while ((row = csvReader.readNext()) != null) {
			for (int j = 1; j < row.length; j++) {
				p.setString(j, row[j]);
			}
			p.addBatch();
			p.clearParameters();
			batchSize++;
			if (batchSize == Constants.BATCH_SIZE) {
				insertResult = p.executeBatch();
				batchSize = 0;
			}
		}
		if (batchSize > 0) {
			insertResult = p.executeBatch();
		}
		return insertResult;
	}
}
