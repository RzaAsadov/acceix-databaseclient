/*
 * The MIT License
 *
 * Copyright 2022 Rza Asadov (rza dot asadov at gmail dot com).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.acceix.ndatabaseclient;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author zrid
 */
public final class DataConnector {
    
    
                public static int MARIADB = 1;
                public static int HBASE = 2;
                public static int UNKNOWN_DB = 100;
                
                
                private boolean useSameConnection = false;
                
                private final Map<String,Object> envs;
                
                private int dbtype;
                
                private Connection connection = null;
                
                private String username;


                public boolean isUseSameConnection() {
                    return useSameConnection;
                }

                public String getUsername() {
                    return username;
                }



                public DataConnector(Map<String, Object> connectionParams,String username) {
                        this.envs = connectionParams;
                        this.username = username;
                        detectDbType();
                }
                
                public DataConnector(Map<String, Object> connectionParams,boolean useSameConnection,String username) {
                        this.envs = connectionParams;
                        this.username = username;
                        detectDbType();
                        this.useSameConnection = useSameConnection;
                }                

                public DataConnector(String dbhost, String schename,String db_username,String db_password,String username) {
                    this.envs = null;
                    this.username = username;
                    detectDbType();
                }
                
                public void detectDbType() {
                    if (envs.get("database_driver").equals("org.mariadb.jdbc.Driver")) {
                            dbtype = MARIADB;
                        } else if (envs.get("database_driver").equals("org.apache.phoenix.jdbc.PhoenixDriver")) {
                            dbtype = HBASE;
                        } else {
                            dbtype = UNKNOWN_DB;
                            throw new UnknownError("Unknown database type");
                        }                    
                }
                

                public int getDbtype() {
                    return dbtype;
                }


                public Connection getConnection() throws ClassNotFoundException, SQLException  {
                    
                        if (getDbtype()==MARIADB) {

                                connection = getConnectionMariadb();

                        } else if (getDbtype()==HBASE) {
                            try {
                                connection = getConnectionHbase();
                            } catch (ClassNotFoundException ex) {
                                Logger.getLogger(DataConnector.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        } else {
                            dbtype = UNKNOWN_DB;
                            throw new UnknownError("Unknown database type");
                        }                    
                    return connection;
                    
                }
                
                public void closeConnection() {
                    try {
                        connection.close();
                        connection=null;
                    } catch (SQLException ex) {
                        Logger.getLogger(DataConnector.class.getName()).log(Level.SEVERE, null, ex);
                        connection=null;
                    }
                }
                
                public boolean isDbConnected() {
                    final String CHECK_SQL_QUERY = "SELECT 1";
                    boolean isConnected = false;
                    try {
                        final PreparedStatement statement = connection.prepareStatement(CHECK_SQL_QUERY);
                        isConnected = true;
                    } catch (SQLException | NullPointerException e) {
                        // handle SQL error here!
                    }
                    return isConnected;
                }                
                
                

                private Connection getConnectionMariadb()  {
                            
                            if ( useSameConnection && isDbConnected()) {
                                return connection;
                            } else {

                                try {
                                    Class.forName(envs.get("database_driver").toString());
                                } catch (ClassNotFoundException ex) {
                                    Logger.getLogger(DataConnector.class.getName()).log(Level.SEVERE, null, ex);
                                }


                                String connectionString = "jdbc:mariadb://" + envs.get("database_host") + ":" + envs.get("database_port") + "/" + envs.get("database_schema") + "?user=" + envs.get("database_user") + "&password=" + envs.get("database_password")  +  "&useUnicode=yes;characterEncoding=UTF-8";// "&minPoolSize=20&maxPoolSize=1000&maxIdleTime=80&pool"
                                
                                try {
                                    connection = DriverManager.getConnection(connectionString);
                                    connection.setAutoCommit(true);
                                    return connection;
                                } catch (SQLException ex) {
                                    Logger.getLogger(DataConnector.class.getName()).log(Level.SEVERE, null, ex);
                                    return null;
                                }

                                        

                            }
                            
                            
                }


                private synchronized Connection getConnectionHbase() throws ClassNotFoundException {


                                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver" );

                                if (!envs.get("phoenix_schema").toString().toLowerCase().equals("default")) {

                                        Properties props = new Properties();
                                        props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
                                        try { 
                                            connection = DriverManager.getConnection("jdbc:phoenix:" +envs.get("phoenix_host"),props);
                                            connection.setAutoCommit(true);
                                            getPreparedStatement(connection, "use " + envs.get("phoenix_schema")).execute();                                             
                                        } catch (SQLException ex) {
                                            Logger.getLogger(DataConnector.class.getName()).log(Level.SEVERE, null, ex);
                                        }

   
                                } else {
                                        
                                        try {
                                            connection = DriverManager.getConnection("jdbc:phoenix:" +envs.get("phoenix_host"));
                                            connection.setAutoCommit(true);                                        
                                        } catch (SQLException ex) {
                                            Logger.getLogger(DataConnector.class.getName()).log(Level.SEVERE, null, ex);
                                        }


                                }
                                return connection;
                }
                
                // Security checked
                public void executeStatement(String sqlStatement) throws MachineDataException, ClassNotFoundException, SQLException {
                        Statement stmt = getConnection().createStatement();
                        stmt.execute(sqlStatement);
                        stmt.close();
                }
                
                // Security checked
                public MachineDataSet executeQuery(String sqlStatement) throws MachineDataException, ClassNotFoundException, SQLException {
                        return new ResultSetConverter().resultSetToMachineDataSet(getConnection().createStatement().executeQuery(sqlStatement));
                }                
                 
                private PreparedStatement getPreparedStatement(Connection c,String sql) throws SQLException {
                        return c.prepareStatement(sql);
                }          
                
                
                public DataTable getTable(String tableName) {
                        return new DataTable(tableName, this);
                }

    
}
