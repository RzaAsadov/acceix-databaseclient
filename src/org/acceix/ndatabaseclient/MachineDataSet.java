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

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;
import java.util.LinkedList;
import java.util.Map;

/**
 *
 * @author Rza Asadov <rza.asadov at gmail.com>
 */
public class MachineDataSet {
    
 
    
    private LinkedList<Map<String,Object>> resultMapList = null;
    private LinkedList<Map<String,Integer>> columnTypeList = null;

    private int curElement = -1;

    public void setResultMapList(LinkedList<Map<String, Object>> resultMapList) {
        this.resultMapList = resultMapList;
    }

    public void setColumnTypeList(LinkedList<Map<String, Integer>> columnTypeList) {
        this.columnTypeList = columnTypeList;
    }

    
    
    public Integer getColumnType(String columnName) {
        if (columnTypeList.get(curElement).get(columnName.toLowerCase())==null) {
            return DataTypes.UNKNOWN_TYPE;
        } 
        return columnTypeList.get(curElement).get(columnName.toLowerCase());
    }

    public LinkedList<Map<String, Object>> getResultAsMap() {
        return resultMapList;
    }
    
    
    public int size() {
        return resultMapList.size();
    }

    public void beforeFirst() {
        curElement = -1;
    }
    
    public void removeElement(Map<String, Object> elementMap) {
        resultMapList.remove(elementMap);
    }
    
   
    public boolean next() {
        if (resultMapList.isEmpty()) {
            return false;
        }
        if (curElement < resultMapList.size()-1) {
            curElement = curElement + 1;
            return true;
        } else {
            return false;
        }
    } 
    
    public Map<String, Object> getMap() {
        return resultMapList.get(curElement);
    }
    
    public int getFirstInt(String columnName) {
        return (int)resultMapList.get(0).get(columnName.toLowerCase());
    }  
    
    public String getFirstString(String columnName) {
        if (resultMapList.size() > 0) {
            return (String)resultMapList.get(0).get(columnName.toLowerCase());
        } else {
            return null;
        }
    }  
    
    public Object getFirst(String columnName) {
        if (resultMapList.size() > 0) {
            return resultMapList.get(0).get(columnName.toLowerCase());
        } else {
            return null;
        }
    }    
    
    public Integer getInteger(String columnName) {
        Object res = resultMapList.get(curElement).get(columnName.toLowerCase());
        if (res==null) {
            return null;
        } else {
            return (Integer)res;
        }
    }

    public Boolean getBoolean(String columnName) {
        Object res = resultMapList.get(curElement).get(columnName.toLowerCase());
        if (res==null) {
            return null;
        } else {
            return (Boolean)res;
        }
    }
    
    public long getLong(String columnName) {
        return (long)resultMapList.get(curElement).get(columnName.toLowerCase());
    }
    
    public double getFloat(String columnName) {
        return (float)resultMapList.get(curElement).get(columnName.toLowerCase());
    }     
    
    public double getDouble(String columnName) {
        return (double)resultMapList.get(curElement).get(columnName.toLowerCase());
    }    

    public BigDecimal getBigDecimal(String columnName) {
        Object res = resultMapList.get(curElement).get(columnName.toLowerCase());
        if (res instanceof BigDecimal) {
            return (BigDecimal) res;            
        } else {
            return new BigDecimal((Integer)res);
        }
    }    
    
    public String getString(String columnName) {
        return (String)resultMapList.get(curElement).get(columnName.toLowerCase());
    }
    
    public String getText(String columnName) {
        return (String)resultMapList.get(curElement).get(columnName.toLowerCase());
    }    
    
    public Timestamp getTimestamp(String columnName) {
        return (Timestamp) resultMapList.get(curElement).get(columnName.toLowerCase());
    }    

    public Date getDate(String columnName) {
            return (Date) resultMapList.get(curElement).get(columnName.toLowerCase());
    }    

    public Time getTime(String columnName) {
        return (Time) resultMapList.get(curElement).get(columnName.toLowerCase());
    }      

    public String getEnum(String columnName) {
        return (String) resultMapList.get(curElement).get(columnName.toLowerCase());
    }    


    public byte[] getBytes(String columnName) {
        return (byte[])resultMapList.get(curElement).get(columnName.toLowerCase());
    }

    
    
    
}
