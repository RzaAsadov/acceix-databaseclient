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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author zrid
 */
public class DataComparable {
    
        public static final int EQUAL=0,GREATER=1,LOWER=2,GREATEROREQUAL=3,LOWEROREQUAL=4,NOTEQUAL=5,COLUMN=6;
        
        private final Map<String,Map<Integer,Object> > columnsToCompare = new LinkedHashMap<>();    
        
        private DataSelectable nDataSelectable;
        private DataDeletable nDataDelete;  
        private DataUpdatable nDataUpdatable;
        private DataInsertable nDataInsertable;
        
        private String tableName;
        

        
        private int sqlcommand=0;

        
        public DataComparable(DataSelectable nDataSelectable) {
            this.nDataSelectable = nDataSelectable;
            sqlcommand=DataExecutable.SELECT;
        }


        public DataComparable(DataDeletable nDataDelete) {
            this.nDataDelete = nDataDelete;
            sqlcommand=DataExecutable.DELETE;
        }

        public DataComparable(DataUpdatable nDataUpdatable) {
            this.nDataUpdatable = nDataUpdatable;
            sqlcommand=DataExecutable.UPDATE;
        } 
        
        public DataComparable(DataInsertable nDataInsertable) {
            this.nDataInsertable = nDataInsertable;
            sqlcommand=DataExecutable.INSERT;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }
        
      
         
        public DataComparable ne (String column,Object value) {
                column = column.toLowerCase();                    
                        Map<Integer,Object> data = new HashMap<>();
                        data.put(NOTEQUAL, value);
                        
                        columnsToCompare.put(tableName + "." + column, data);
                
                return this;
        }           
        
        public DataComparable eq (String column,Object value) {
                column = column.toLowerCase();                    
                        Map<Integer,Object> data = new HashMap<>();
                        data.put(EQUAL, value);
                         
                        columnsToCompare.put(tableName + "." + column, data);
                
                return this;
        }
       
        
        public DataComparable gt (String column,Object value) {
                column = column.toLowerCase();                    
                        Map<Integer,Object> data = new HashMap<>();
                        data.put(GREATER, value);
                        
                        columnsToCompare.put(tableName + "." + column, data);
                
                return this;
        }     

        public DataComparable lt (String column,Object value) {

                column = column.toLowerCase();                    
                        Map<Integer,Object> data = new HashMap<>();
                        data.put(LOWER, value);
                        
                        columnsToCompare.put(tableName + "." + column, data);
                
                return this;
        }   
        
       
    
        public DataComparable ge (String column,Object value) {
            
                column = column.toLowerCase();                    
                        Map<Integer,Object> data = new HashMap<>();
                        data.put(GREATEROREQUAL, value);
                        
                        columnsToCompare.put(tableName + "." + column, data);

                return this;
                
        }     
                
        
        public DataComparable le (String column,Object value) {
            
                column = column.toLowerCase();                    
                        Map<Integer,Object> data = new HashMap<>();
                        data.put(LOWEROREQUAL, value);
                        
                        columnsToCompare.put(tableName + "." + column, data);
                
                return this;
        }             
        
        
        public DataComparable orderByAsc(String orderByColumn) {
            orderByColumn = orderByColumn.toLowerCase();
            nDataSelectable = nDataSelectable.orderByAsc(orderByColumn);
            return this;
        }        

        public DataComparable orderByDesc(String orderByColumn) {
            orderByColumn = orderByColumn.toLowerCase();            
            nDataSelectable = nDataSelectable.orderByDesc(orderByColumn);
            return this;
        }        
        
        public DataComparable groupBy(String column) {
            column = column.toLowerCase();
            nDataSelectable = nDataSelectable.groupBy(column);
            return this;
        }
        
        
        public DataComparable setLimit(String limit) {
            nDataSelectable.setLimit(limit);
            return this;
        }

         
        public DataExecutable compile () throws ClassNotFoundException, SQLException {
            
            switch (sqlcommand) {
                case DataExecutable.UPDATE:
                    nDataUpdatable.setColumnsToCompare(columnsToCompare);
                    return nDataUpdatable.compile();
                case DataExecutable.DELETE:
                    nDataDelete.setColumnsToCompare(columnsToCompare);
                    return nDataDelete.compile();
                case DataExecutable.INSERT:
                    return nDataInsertable.compile();
                case DataExecutable.SELECT:
                    nDataSelectable.setColumnsToCompare(columnsToCompare);
                    return nDataSelectable.compile();
                default:
                    return null;
            }

        }
        
    
        
}
