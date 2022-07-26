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
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.StringJoiner;
import org.acceix.logger.NLog;
import org.acceix.logger.NLogBlock;
import org.acceix.logger.NLogger;

/**
 *
 * @author zrid
 */
public class DataDeletable {
    
    
    
        private final DataTable ndataTable;

        
        private boolean debug=false;
        
        
        private Integer deleteID = null;


        private final DataComparable nDataComparable;        
        
        private Map<String,Map<Integer,Object> > columnsToCompare;    

        
        final StringJoiner compareListJoiner = new StringJoiner(" AND ");          
        
        
        private PreparedStatement preparedStatement;
        final StringBuilder resultQuery = new StringBuilder();        

        public DataDeletable(DataTable ndataTable,Integer id) {
                this.ndataTable = ndataTable;
                deleteID = id;
                nDataComparable = new DataComparable(this);
                this.nDataComparable.setTableName(ndataTable.getFirstTable()); 
        }   
        
        public DataDeletable(DataTable ndataTable) {
                this.ndataTable = ndataTable;
                nDataComparable = new DataComparable(this);
                this.nDataComparable.setTableName(ndataTable.getFirstTable());                
        }           

        public void setDebug(boolean debug) {
            this.debug = debug;
        }



        public Integer getDeleteID() {
            return deleteID;
        }

        public DataComparable where () {
                return nDataComparable;
        }

        public void setColumnsToCompare(Map<String, Map<Integer,Object> > columnsToCompare) {
            this.columnsToCompare = columnsToCompare;
        }        
        
        
        public DataExecutable compile() throws SQLException, ClassNotFoundException {
           

                    
                    if (getDeleteID()!=null) {
                        
                    }
                    
                    resultQuery
                        .append("delete from ")
                        .append(ndataTable.getFirstTable())
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
                        NLogger.logger(NLogBlock.DB,NLog.MESSAGE,"ScriptLoader","load","system",resultQuery.toString());

                preparedStatement = ndataTable.getnDataConnector().getConnection().prepareStatement(resultQuery.toString());
                
                int index=1;
                
                    for (Map.Entry<String,Map<Integer,Object>> columnMap : columnsToCompare.entrySet()) {                   

                            for (Map.Entry<Integer,Object> fieldset : columnMap.getValue().entrySet()) {        

                                    if (fieldset.getValue() instanceof Integer) preparedStatement.setInt(index, (Integer)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof Long) preparedStatement.setLong(index, (Long)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof Long) preparedStatement.setFloat(index, (Float)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof Long) preparedStatement.setDouble(index, (Double)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof String) preparedStatement.setString(index, (String)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof String) preparedStatement.setBoolean(index, (Boolean)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof String) preparedStatement.setDate(index, (Date)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof String) preparedStatement.setTime(index, (Time)fieldset.getValue());
                                    else if (fieldset.getValue() instanceof Timestamp) preparedStatement.setTimestamp(index, (Timestamp)fieldset.getValue());
                                    
                            }

                            index++;
                    }  
                                        
                return new DataExecutable(preparedStatement, ndataTable.getnDataConnector(), DataExecutable.DELETE);
            
        }        
 

}
