package sa.edu.kaust.twitter.dbconnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySqlConnection {
	
    Connection conn;
    
    public Connection GetConnection(String ipServer,String username,
      String password)throws SQLException,IllegalStateException{
        DestroyConnection();
        String dbDriver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://" +ipServer +"/twitterMyIsam";
        try {
            Class.forName(dbDriver);
            conn = DriverManager.getConnection(dbURL,username,password);
            return conn;
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return conn;
    }
    
    public void DestroyConnection() 
    throws SQLException,IllegalStateException{
        if(conn!=null){
            conn.close();
            conn = null;
        }
    }
    
    public boolean isConnected()
    {
        if(conn!=null)
            return true;
        return false;
    }
    
    public ResultSet ExecuteSelect(String query)
    throws SQLException,IllegalStateException{
        Statement stm;
        ResultSet rs = null;
        
        if(conn==null)
            return null;
        stm = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        rs = stm.executeQuery(query);
        return rs;
    }
    
    public int ExecuteNonQuery(String query) 
    throws SQLException,IllegalStateException{
        Statement stmt;
      
        if(conn==null)
            return 0;
        stmt = conn.createStatement();
      
        return stmt.executeUpdate(query);
    }
    
    public PreparedStatement StatementPreparation(String query) 
    throws SQLException,IllegalStateException{
        PreparedStatement pstmt = conn.prepareStatement(query);
        return pstmt;
    }
}


