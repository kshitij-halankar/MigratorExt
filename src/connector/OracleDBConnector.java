package connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleDBConnector {

	public Connection getConnection(String dbURL, String dbUser, String dbPassword) throws SQLException {
		Connection con = DriverManager.getConnection(dbURL, dbUser, dbPassword);
		return con;
	}
}
