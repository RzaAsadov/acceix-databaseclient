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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;
import org.acceix.logger.NLog;
import org.acceix.logger.NLogBlock;
import org.acceix.logger.NLogger;

/**
 *
 * @author zrid
 */
public class DataInsertable {
     
    
        private final DataTable ndataTable;

        private boolean debug=false;
        
        private final Map<String,Object> columnsToInsert= new LinkedHashMap<>();
        
        
        private PreparedStatement preparedStatement;
        final StringBuilder resultQuery = new StringBuilder();        

        public DataInsertable(DataTable ndataTable) {
                this.ndataTable = ndataTable;
        }     

        public void setDebug(boolean debug) {
            this.debug = debug;
        }

        
        
        public DataInsertable add(String column,Object value) {
                column = column.toLowerCase();
                columnsToInsert.put(column,value);
                return this;
        }
        
        public boolean isColumnExists(String column) {
            return columnsToInsert.get(column) != null;
                  
        }
        
        
        public DataExecutable compile() throws SQLException, ClassNotFoundException {
            

                final StringJoiner columnsQueryPart = new StringJoiner(",","(",")");
                final StringJoiner valuesQueryPart = new StringJoiner(",","(",")");



                
                
                    for (Map.Entry<String,Object> entry : columnsToInsert.entrySet()) {
                                    columnsQueryPart.add(entry.getKey().toUpperCase());
                                    valuesQueryPart.add("?");                        

                    }
                

                    resultQuery.append("INSERT INTO ");
                                
                
                
                
                    resultQuery.append(ndataTable.getFirstTable())
                               .append(columnsQueryPart.toString())
                               .append(" values ").append(valuesQueryPart.toString());
                
                    if (debug)
                        NLogger.logger(NLogBlock.DB,NLog.MESSAGE,"DataInsertable","compile",ndataTable.getnDataConnector().getUsername(),resultQuery.toString());
                    
                preparedStatement = ndataTable.getnDataConnector().getConnection().prepareStatement(resultQuery.toString(),Statement.RETURN_GENERATED_KEYS);
                
                int index=1;
                
                for (Map.Entry<String,Object> entry : columnsToInsert.entrySet()) {
                    
                            Object value = entry.getValue();

                
                            if (value instanceof Integer) {
                                    preparedStatement.setInt(index, (Integer)value);
                            } else if (value instanceof Long) {
                                    preparedStatement.setLong(index, (Long)value);
                            } else if (value instanceof Float) {
                                    preparedStatement.setFloat(index, (Float)value);
                            } else if (value instanceof Double) {
                                    preparedStatement.setDouble(index, (Double)value);
                            } else if (value instanceof String) {
                                    preparedStatement.setString(index, (String)value);
                            } else if (value instanceof Timestamp) {
                                    preparedStatement.setTimestamp(index, (Timestamp)value);
                            } else if (value instanceof Date) {
                                    preparedStatement.setDate(index, (Date)value);
                            } else if (value instanceof Time) {
                                    preparedStatement.setTime(index, (Time)value);
                            } else if (value instanceof Boolean) {
                                    preparedStatement.setBoolean(index, (Boolean)value);
                            } else if (value instanceof byte[]) {
                                    preparedStatement.setBytes(index, (byte[])value);
                            }
                            index++;
                                            
                }
                                        
                return new DataExecutable(preparedStatement,ndataTable.getnDataConnector(), DataExecutable.INSERT);
            
        }            
    
}
