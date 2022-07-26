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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Date;
import java.sql.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import org.acceix.logger.NLog;
import org.acceix.logger.NLogBlock;
import org.acceix.logger.NLogger;

/**
 *
 * @author zrid
 */
public class DataUpdatable {
    
     
        private final DataTable ndataTable;
        
        private final DataComparable nDataComparable;
        
        private int dbtype;
        private boolean debug=false;
                 
        private Integer updateID = null;

        
        private final Map<String,Object> columnsToUpdate = new HashMap<>();
        private Map<String,Map<Integer,Object> > columnsToCompare;     
        
        
        
        private PreparedStatement preparedStatement;
        final StringBuilder resultQuery = new StringBuilder();       

        public void setDebug(boolean debug) {
            this.debug = debug;
        }

        public DataUpdatable(DataTable ndataTable,Integer id) {
                this.ndataTable = ndataTable;
                this.nDataComparable = new DataComparable(this);    
                this.nDataComparable.setTableName(ndataTable.getFirstTable());                
                updateID = id;
        }     
        
        public DataUpdatable(DataTable ndataTable) {
                this.ndataTable = ndataTable;
                this.nDataComparable = new DataComparable(this);    
                this.nDataComparable.setTableName(ndataTable.getFirstTable());                 
        }     
        
        public void setColumnsToCompare(Map<String, Map<Integer,Object> > columnsToCompare) {
            this.columnsToCompare = columnsToCompare;
        }        
        


        public Integer getUpdateID() {
            return updateID;
        }


        public DataComparable where () {                
                return nDataComparable;
        }

        public Map<String, Object> getColumnsToUpdate() {
            return columnsToUpdate;
        }


        public DataUpdatable update(String column,Object value) {
                
                columnsToUpdate.put(column, value);
                return this;
        }        
        
        
        public DataUpdatable updateInteger(String column,Integer value) {
                column = column.toLowerCase();
                columnsToUpdate.put(column, value);
                return this;
        }
        
        public DataUpdatable updateLong(String column,Long value) {
                column = column.toLowerCase();
                columnsToUpdate.put(column, value);
                return this;
        }

        public DataUpdatable updateFloat(String column,Float value) {
                column = column.toLowerCase();
                columnsToUpdate.put(column, value);
                return this;
        }        
        
        public DataUpdatable updateDouble(String column,Double value) {
                column = column.toLowerCase();
                columnsToUpdate.put(column, value);
                return this;
        }        
        
        public DataUpdatable updateVarchar(String column,String value) {
                column = column.toLowerCase();
                columnsToUpdate.put(column, (String)value);
                return this;
        }
        
        public DataUpdatable updateJson(String column,String value) {
                column = column.toLowerCase();            
                columnsToUpdate.put(column, (String)value);
                return this;
        }        
        
        public DataUpdatable updateText(String column,String value) {
                column = column.toLowerCase();            
                columnsToUpdate.put(column, (String)value);
                return this;
        }        

         public DataUpdatable updateTimestamp(String column,Timestamp value) {
                column = column.toLowerCase();             
                columnsToUpdate.put(column, (Timestamp)value);
                return this;
        }
        
        public DataUpdatable updateDatetime(String column,Timestamp value) {
                column = column.toLowerCase();            
                columnsToUpdate.put(column, (Timestamp)value);
                return this;
        }
        
        public DataUpdatable updateByte(String column,byte[] value) {
                column = column.toLowerCase();            
                columnsToUpdate.put(column, (byte[])value);
                return this;
        }
        
        public DataUpdatable updateBoolean(String column,boolean value) {
                column = column.toLowerCase();            
                columnsToUpdate.put(column, (boolean)value);
                return this;
        }    
        
        public DataUpdatable updateDate (String column,Date date) {
                column = column.toLowerCase();            
                columnsToUpdate.put(column, (Date)date);
                return this;
        }
        
        public DataUpdatable updateTime (String column,Time date) {
                column = column.toLowerCase();            
                columnsToUpdate.put(column, (Time)date);
                return this;
        }        
        
        
        
        public DataExecutable compile() throws SQLException, ClassNotFoundException {


                final StringJoiner columnsToUpdateStrBuilder;

                final StringJoiner compareListJoiner = new StringJoiner(" AND ");                

                    
                    columnsToUpdateStrBuilder = new StringJoiner(",");
                    //valuesQueryPart = new StringJoiner(",","(",")");

                    if (getUpdateID()!=null) {
                        compareListJoiner.add("id" + " = ?");
                    }

                    getColumnsToUpdate().forEach((k,v) -> {
                        columnsToUpdateStrBuilder.add(k + " = ?");
                    });
                    
                    resultQuery
                        .append("update ")
                        .append(ndataTable.getFirstTable())
                        .append(" set ")
                        .append(columnsToUpdateStrBuilder.toString())
                        .append(" where ");
                    
                        columnsToCompare.entrySet().forEach((entry) -> {
                            Map<Integer,Object> entryValue = entry.getValue();

                            entryValue.entrySet().forEach((element) -> {

                                    if (null!=element.getKey()) switch (element.getKey()) {
                                    case DataComparable.EQUAL:
                                        compareListJoiner.add(entry.getKey() + " = ?");
                                        break;
                                    case DataComparable.GREATER:
                                        compareListJoiner.add(entry.getKey() + " > ?");
                                        break;
                                    case DataComparable.GREATEROREQUAL:
                                        compareListJoiner.add(entry.getKey() + " >= ?");
                                        break;
                                    case DataComparable.LOWER:
                                        compareListJoiner.add(entry.getKey() + " < ?");
                                        break;
                                    case DataComparable.LOWEROREQUAL:
                                        compareListJoiner.add(entry.getKey() + " <= ?");
                                        break;
                                    default:
                                        break;
                                    }

                             });



                        });
                
                    resultQuery.append(compareListJoiner);
                
                            
                
                if (debug)
                    NLogger.logger(NLogBlock.DB,NLog.MESSAGE,"DataUpdatable","compile",ndataTable.getnDataConnector().getUsername(),resultQuery.toString());


                preparedStatement = ndataTable.getnDataConnector().getConnection().prepareStatement(resultQuery.toString());
                
                
                int index=1;
                
                    for (Map.Entry<String,Object> entry : columnsToUpdate.entrySet()) {

                                Object value = entry.getValue();

                                if (value instanceof Integer) preparedStatement.setInt(index, (Integer)value);
                                else if (value instanceof Long) preparedStatement.setLong(index, (Long)value);                                
                                else if (value instanceof Float) preparedStatement.setFloat(index, (Float) value);
                                else if (value instanceof Double) preparedStatement.setDouble(index, (Double) value);
                                else if (value instanceof String) preparedStatement.setString(index, (String)value);
                                else if (value instanceof Timestamp) preparedStatement.setTimestamp(index, (Timestamp)value);
                                else if (value instanceof byte[]) preparedStatement.setBytes(index, (byte[])value);
                                else if (value instanceof Boolean) preparedStatement.setBoolean(index, (Boolean)value);
                                else if (value instanceof Date) preparedStatement.setDate(index, (Date) value);
                                else if (value instanceof Time) preparedStatement.setTime(index, (Time) value);
                                
                                index++;
                    }
                
           

                    for (Map.Entry<String,Map<Integer,Object>> columnMap : columnsToCompare.entrySet()) {                   

                            for (Map.Entry<Integer,Object> fieldset : columnMap.getValue().entrySet()) {        

                                    if (fieldset.getValue() instanceof Integer) preparedStatement.setInt(index, (Integer)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof String) preparedStatement.setString(index, (String)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof Long) preparedStatement.setLong(index, (Long)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof Timestamp) preparedStatement.setTimestamp(index, (Timestamp)fieldset.getValue());
                                    
                            }

                            index++;
                    }                
                                        
                return new DataExecutable(preparedStatement,ndataTable.getnDataConnector(), DataExecutable.UPDATE);
        }        
 

                
    
}
