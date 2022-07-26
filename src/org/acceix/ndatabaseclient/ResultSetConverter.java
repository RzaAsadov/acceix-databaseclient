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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_BIGDECIMAL;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_BOOLEAN;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_BYTES;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_DATE;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_DATETIME;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_DOUBLE;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_ENUM;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_FLOAT;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_INT;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_JSON;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_LONG;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_STRING;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_TEXT;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_TIME;
import static org.acceix.ndatabaseclient.DataTypes.TYPE_TIMESTAMP;
 
/**
 *
 * @author zrid
 */
public class ResultSetConverter {
    

    
    
    public MachineDataSet resultSetToMachineDataSet (ResultSet resultSet) throws SQLException {
        
            LinkedList<Map<String,Object>> resultDataMapList = new LinkedList<>();
            LinkedList<Map<String,Integer>> resultDataTypeList = new LinkedList<>();
            
            MachineDataSet machineDataSet = new MachineDataSet();

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columncount = metaData.getColumnCount(); //number of column

            String columnNames[] = new String[columncount];
            int columnTypes[] = new int[columncount];
            
            for (int i=0; i < columncount; i++) {
                
                    columnNames [i] = metaData.getColumnLabel(i+1);

                    String columntType = metaData.getColumnTypeName(i+1);

                    //System.out.println(columnNames[i] + " type is " + metaData.getColumnTypeName(i+1));                    
                    
                    if ( (columntType.equals("INTEGER")) || (columntType.equals("TINYINT")) || (columntType.equals("INTEGER UNSIGNED")) ) {
                         columnTypes[i] = TYPE_INT;
                    } else if (columntType.equals("BIGINT")) {
                         columnTypes[i] = TYPE_LONG;
                    } else if (columntType.equals("BIT")) {
                         columnTypes[i] = TYPE_BOOLEAN;
                    } else if (columntType.equals("FLOAT")) {
                         columnTypes[i] = TYPE_FLOAT;
                    } else if (columntType.equals("DOUBLE")) {
                         columnTypes[i] = TYPE_DOUBLE;
                    } else if (columntType.equals("DECIMAL")) {
                         columnTypes[i] = TYPE_BIGDECIMAL;
                    } else if ( (columntType.equals("CHAR")) ||  (columntType.equals("VARCHAR")) ) {
                         columnTypes[i] = TYPE_STRING;
                    } else if (columntType.equals("TEXT")) {
                         columnTypes[i] = TYPE_TEXT;
                    } else if (columntType.equals("TIMESTAMP")) {
                         columnTypes[i] = TYPE_TIMESTAMP;
                    } else if (columntType.equals("VARBINARY")) {
                         columnTypes[i] = TYPE_BYTES;
                    } else if (columntType.equals("BLOB")) {
                         columnTypes[i] = TYPE_BYTES;                         
                    } else if (columntType.equals("INTERVAL DAY TO SECOND")) {
                         columnTypes[i] = TYPE_STRING;
                    } else if (columntType.equals("DATE")) {
                         columnTypes[i] = TYPE_DATE;
                    } else if (columntType.equals("TIME")) {
                         columnTypes[i] = TYPE_TIME;
                    } else if (columntType.equals("DATETIME")) {
                         columnTypes[i] = TYPE_DATETIME;
                    } else if (columntType.equals("ENUM")) {
                         columnTypes[i] = TYPE_ENUM;
                    }                

            }
            
            while (resultSet.next()) {
                
                Map <String,Object> rowDataMap = new LinkedHashMap<>();
                Map <String,Integer> rowDataTypes = new LinkedHashMap<>();
                
                for (int i=0; i < columnNames.length; i++) {
                    //System.out.println(columnNames[i] + " is " + columnTypes[i]);
                    switch (columnTypes[i]) {
                        case TYPE_INT:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getInt(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;
                        case TYPE_BOOLEAN:                            
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getBoolean(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;                            
                        case TYPE_LONG:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getLong(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;
                        case TYPE_FLOAT:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getFloat(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;
                        case TYPE_DOUBLE:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getDouble(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;                            
                        case TYPE_BIGDECIMAL:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getBigDecimal(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;                            
                        case TYPE_STRING:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getString(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;
                        case TYPE_TEXT:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getString(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;
                        case TYPE_JSON:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getString(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;                            
                        case TYPE_TIMESTAMP:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getTimestamp(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;                            
                        case TYPE_BYTES:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getBytes(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;
                        case TYPE_DATE:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getDate(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);                            
                            break;                            
                        case TYPE_TIME:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getTime(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;         
                        case TYPE_DATETIME:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getTimestamp(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break; 
                        case TYPE_ENUM:
                            rowDataMap.put(columnNames[i].toLowerCase(), resultSet.getString(columnNames[i]));
                            rowDataTypes.put(columnNames[i].toLowerCase(), columnTypes[i]);
                            break;                              
                            
                            
                        default:
                            break;
                    }
                  
                }
                resultDataMapList.add(rowDataMap);
                resultDataTypeList.add(rowDataTypes);
            }
            
           
            machineDataSet.setColumnTypeList(resultDataTypeList);
            machineDataSet.setResultMapList(resultDataMapList);
            
            
            return machineDataSet;
            
    }
    
}
