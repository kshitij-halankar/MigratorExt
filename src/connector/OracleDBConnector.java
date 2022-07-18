package connector;

import java.sql.Connection;
import java.sql.DriverManager;

public class OracleDBConnector {

	String dbURL = "";
	String dbUsername = "";
	String dbPass = "";

//	try (Connection conn = DriverManager.getConnection(
//            "jdbc:oracle:thin:@localhost:1521:orcl", "system", "Password123")) {
//
//        if (conn != null) {
//            System.out.println("Connected to the database!");
//        } else {
//            System.out.println("Failed to make connection!");
//        }
//
//    } catch (SQLException e) {
//        System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
//    } catch (Exception e) {
//        e.printStackTrace();
//    }

	public void setDBURL(String dbURL) {
		this.dbURL = dbURL;
	}

	public void setDBUsername(String dbUsername) {
		this.dbUsername = dbUsername;
	}

	public void setDBPass(String dbPass) {
		this.dbPass = dbPass;
	}

	public Connection getConnection() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(dbURL, dbUsername, dbPass);
		} catch (Exception ex) {

		}
		return conn;
	}
}
