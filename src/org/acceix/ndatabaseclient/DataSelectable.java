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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.acceix.logger.NLog;
import org.acceix.logger.NLogBlock;
import org.acceix.logger.NLogger;

/**
 *
 * @author zrid
 */
public class DataSelectable {
    
        private final StringBuilder selectStringBuilder = new StringBuilder("SELECT ");
    
        private final DataTable ndataTable;
        
        private final DataComparable nDataComparable;
        
        private final List<String> columnsToSelect = new ArrayList<>();
        private Map<String,Map<Integer,Object> > columnsToCompare = new LinkedHashMap<>();
        private Map<String,Map<Integer,Object> > joinedONColumns = new LinkedHashMap<>();
        private String addQuery=null;
        
        private final List<String> orderByColumns = new LinkedList<>();
        private final List<String> groupByColumns = new LinkedList<>();
        private String orderbyDirection = "asc";
        
        private String limit = "";
        
        private boolean debug=false;
        
        
        private PreparedStatement preparedStatement;

        public DataSelectable(DataTable ndataTable) {
                this.ndataTable = ndataTable;
                this.nDataComparable = new DataComparable(this);    
                this.nDataComparable.setTableName(ndataTable.getFirstTable());
        }
 
        public DataSelectable setDebug(boolean debug) {
            this.debug = debug;
            return this;
        }
        
        public DataSelectable setAddQuery(String addQuery) {
            this.addQuery = addQuery;
            return this;
        }

        public DataSelectable joinTable(String tableName,String idFieldOnLocalTable,String idFieldOnJoinedTable) {
            
            Map<Integer,Object> data = new HashMap<>();
            data.put(DataComparable.COLUMN, ndataTable.getFirstTable() + "." + idFieldOnLocalTable);
                        
            joinedONColumns.put(tableName + "." + idFieldOnJoinedTable, data);
            
            ndataTable.joinTable(tableName);
            return this;
            
        }
        
        public DataSelectable joinTable(String tableName,String idFieldOnLocalTable,String idFieldOnJoinedTable,Object noChoiceValueForMainTable) {
            
            Map<Integer,Object> data = new HashMap<>();
            data.put(DataComparable.COLUMN, ndataTable.getFirstTable() + "." + idFieldOnLocalTable);
                        
            joinedONColumns.put(tableName + "." + idFieldOnJoinedTable, data);
            
            //////////////////////////// for no choice option ////////////////////////////
            
            Map<Integer,Object> dataForNoChoice = new HashMap<>();
            dataForNoChoice.put(DataComparable.EQUAL, Integer.valueOf(0));
                        
            joinedONColumns.put(ndataTable.getFirstTable() + "." + idFieldOnLocalTable, dataForNoChoice); 
            
            //////////////////////////////////////////////////////////////////////////////
            
            ndataTable.joinTable(tableName);
            return this;
            
        }        
        
        public DataSelectable endJoin() {
            ndataTable.endJoin();
            return this;
        } 

        public DataSelectable getColumn(String column) {
                column = column.toLowerCase();
                columnsToSelect.add(ndataTable.getLastTable() + "." + column + " AS " + column);
                return this;
        }
        
        public DataSelectable getColumnAs(String column,String requestAs) {
                column = column.toLowerCase();
                columnsToSelect.add(ndataTable.getLastTable() + "." + column + " AS " + requestAs);
                return this;
        }    
        
        public DataSelectable getColumnStatement(String statement,String requestAs) {
                columnsToSelect.add(statement + " AS " + requestAs);
                return this;
        }         

        public void setLimit(String limit) {
            this.limit = limit;
        }

        public String getLimit() {
            return limit;
        }


        
        public DataComparable where () {
                return nDataComparable;
        }

        public void setColumnsToCompare(Map<String, Map<Integer,Object> > columnsToCompare) {
            this.columnsToCompare = columnsToCompare;
        }

        public DataSelectable orderByAsc(String orderByColumn) {
            orderByColumns.add(orderByColumn);
            this.orderbyDirection = "asc";
            return this;
        }

        public DataSelectable orderByDesc(String orderByColumn) {
            orderByColumns.add(orderByColumn);
            this.orderbyDirection = "desc";
            return this;            
        }
        
        public DataSelectable groupBy (String colum) {
            groupByColumns.add(colum);
            return this;
        }


        public DataExecutable compile() throws SQLException, ClassNotFoundException {
            
                
                final StringJoiner columnsJoiner = new StringJoiner(",");
                final StringJoiner compareListJoiner = new StringJoiner(" AND ");

                columnsToSelect.forEach((column) -> { 
                        columnsJoiner.add(column);
                });

                StringJoiner tables_Str = new StringJoiner(",");
                for (String table : ndataTable.getTableNames()) { tables_Str.add(table); }

                selectStringBuilder.append(columnsJoiner.toString())
                                  .append(" FROM ")
                                  .append(tables_Str);
                
                columnsToCompare.putAll(joinedONColumns);
                

                
                if (!columnsToCompare.isEmpty()) {
                
                                selectStringBuilder.append(" WHERE ");
                    
                                columnsToCompare.entrySet().forEach((entry) -> {
                                    Map<Integer,Object> entryValue = entry.getValue();

                                    entryValue.entrySet().forEach((element) -> {

                                         if (null!=element.getKey()) {
                                             
                                                switch (element.getKey()) {
                                                    
                                                   case DataComparable.EQUAL:
                                                       compareListJoiner.add(entry.getKey() + " = ?");
                                                       break;
                                                   case DataComparable.NOTEQUAL:
                                                       compareListJoiner.add(entry.getKey() + " != ?");
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
                                                   case DataComparable.COLUMN:
                                                       compareListJoiner.add(entry.getKey() + " = " + entry.getValue().get(DataComparable.COLUMN));
                                                       break;                                                
                                                   default:
                                                       break;
                                                       
                                                }
                                                
                                         }

                                     });



                                });


                                selectStringBuilder.append(compareListJoiner);
                    
                }                
                
                

                
                if (addQuery != null) {
                    selectStringBuilder.append(" ");
                    selectStringBuilder.append(addQuery);
                    //System.out.println(selectStringBuilder.toString());
                }
                
                if (groupByColumns.size() > 0 ) {
                        selectStringBuilder.append(" group by ");
                        StringJoiner groupByColumnsJoiner = new StringJoiner(",");
                        groupByColumns.forEach((column) -> {
                            groupByColumnsJoiner.add(column);
                        });
                        selectStringBuilder.append(groupByColumnsJoiner.toString());

                }                 
                
                if (orderByColumns.size() > 0 ) {
                        selectStringBuilder.append(" order by ");
                        StringJoiner orderByColumnsJoiner = new StringJoiner(",");
                        orderByColumns.forEach((column) -> {
                            orderByColumnsJoiner.add(column);
                        });
                        selectStringBuilder.append(orderByColumnsJoiner.toString());
                        selectStringBuilder.append(" ");
                        selectStringBuilder.append(orderbyDirection);
                } 
                
                if (limit.length() > 0) {
                    selectStringBuilder.append(" limit ");
                    selectStringBuilder.append(getLimit());
                }
                
                    if (debug)
                        NLogger.logger(NLogBlock.DB,NLog.MESSAGE,"DataSelectable","compile",ndataTable.getnDataConnector().getUsername(),selectStringBuilder.toString());
                    
                    
                    NLogger.logger(NLogBlock.DB,NLog.MESSAGE,"DataSelectable","compile",ndataTable.getnDataConnector().getUsername(),selectStringBuilder.toString());
                    preparedStatement = ndataTable.getnDataConnector().getConnection().prepareStatement(selectStringBuilder.toString());
 

                    int index = 1;                

                    for (Map.Entry<String,Map<Integer,Object>> columnMap : columnsToCompare.entrySet()) {                   

                            for (Map.Entry<Integer,Object> fieldset : columnMap.getValue().entrySet()) {
                                
                                    if (fieldset.getKey()==DataComparable.COLUMN) continue;

                                    if (fieldset.getValue() instanceof Integer) {
                                            preparedStatement.setInt(index, (Integer)fieldset.getValue());
                                    } else if (fieldset.getValue() instanceof Long) {
                                            preparedStatement.setLong(index, (Long)fieldset.getValue());
                                    } else if (fieldset.getValue() instanceof String) {
                                            preparedStatement.setString(index, (String)fieldset.getValue());
                                    } else if (fieldset.getValue() instanceof Timestamp) {
                                            preparedStatement.setTimestamp(index, (Timestamp)fieldset.getValue());
                                    } else if (fieldset.getValue() instanceof Date) {
                                            preparedStatement.setDate(index, (Date)fieldset.getValue());
                                    } else if (fieldset.getValue() instanceof Time) {
                                            preparedStatement.setTime(index, (Time)fieldset.getValue());
                                    } else if (fieldset.getValue() instanceof Float) {
                                            preparedStatement.setFloat(index, (Float)fieldset.getValue());
                                    } else if (fieldset.getValue() instanceof Double) {
                                            preparedStatement.setDouble(index, (Double)fieldset.getValue());
                                    } else if (fieldset.getValue() instanceof Boolean) {
                                            preparedStatement.setBoolean(index, (Boolean)fieldset.getValue());
                                    }

                            }

                            index++;                        

                    }                
                                
                return new DataExecutable(preparedStatement, ndataTable.getnDataConnector(),DataExecutable.SELECT);

        }
        

        
}
