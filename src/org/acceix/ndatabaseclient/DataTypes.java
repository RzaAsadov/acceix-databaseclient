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
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


 
/**
 *
 * @author zrid
 */
public class DataTypes {
    
        public static final int UNKNOWN_TYPE=-100;    
        public static final int TYPE_DOUBLE = 9;
        public static final int TYPE_DOCUMENT = 10;
        public static final int TYPE_LOCATION=18;
        public static final int TYPE_LONG=11;     
        public static final int TYPE_FLOAT=12;    
        public static final int TYPE_STRING=0;        
        public static final int TYPE_INT=1;
        public static final int TYPE_BOOLEAN=2;
        public static final int TYPE_TIMESTAMP=3;        
        public static final int TYPE_JSON = 4;
        public static final int TYPE_ENUM = 5;
        public static final int TYPE_DATE = 6;
        public static final int TYPE_TIME = 7;
        public static final int TYPE_DATETIME = 8;
        public static final int TYPE_BYTES=13;
        public static final int TYPE_TEXT=14;
        public static final int TYPE_LONGTEXT=17;
        public static final int TYPE_BIGDECIMAL=15;   
        public static final int TYPE_MEDIUMTEXT=16;
        public static final int TYPE_PAYLOAD=19;
        public static final int TYPE_FIXED=20;


        

        public int stringToDataType(String dataType) {
                
                switch (dataType) {
                    
                    case "string":
                        return DataTypes.TYPE_STRING;                    

                    case "varchar":
                        return DataTypes.TYPE_STRING;
                        
                    case "char":
                        return DataTypes.TYPE_STRING;
                        
                    case "text":
                        return DataTypes.TYPE_STRING;
                        
                    case "mediumtext":
                        return DataTypes.TYPE_MEDIUMTEXT;
                        
                    case "longtext":
                        return DataTypes.TYPE_JSON;
                                                
                    case "numeric":
                        return DataTypes.TYPE_INT;
                        
                    case "int":
                        return DataTypes.TYPE_INT;
                        
                    case "integer":
                        return DataTypes.TYPE_INT;                        
                        
                    case "int unsigned":
                        return DataTypes.TYPE_INT;

                    case "bigint":
                        return DataTypes.TYPE_INT;  
                        
                    case "smallint":
                        return DataTypes.TYPE_INT;                         
                        
                    case "mediumint":
                        return DataTypes.TYPE_INT;                         

                    case "tinyint":
                        return DataTypes.TYPE_INT;                          
                        
                    case "long":
                        return DataTypes.TYPE_LONG;                        
                        
                    case "float":
                        return DataTypes.TYPE_FLOAT;
                                                  
                    case "double":
                        return DataTypes.TYPE_DOUBLE;
                                                
                    case "bit":
                        return DataTypes.TYPE_BOOLEAN;
                        
                    case "json":
                        return DataTypes.TYPE_JSON;
                        
                    case "document":
                        return DataTypes.TYPE_DOCUMENT; 
                        
                    case "location":
                        return DataTypes.TYPE_LOCATION;                         
                        
                    case "timestamp":
                        return DataTypes.TYPE_TIMESTAMP;
                        
                    case "datetime":
                        return DataTypes.TYPE_TIMESTAMP;
                        
                    case "date":
                        return DataTypes.TYPE_DATE;
                        
                    case "time":
                        return DataTypes.TYPE_TIME;
                        
                    case "enum":
                        return DataTypes.TYPE_ENUM;
                        
                    case "payload":
                        return DataTypes.TYPE_PAYLOAD;
                    case "fixed":
                        return DataTypes.TYPE_FIXED;                         
 
                        
                    default:
                        return DataTypes.UNKNOWN_TYPE;
                        
                }        
        }          
        
        public String dataTypeToString(int dataType) {
            
                if (dataType==DataTypes.TYPE_INT) {
                    return "integer";
                } else if (dataType==DataTypes.TYPE_LONG) {
                    return "long";
                } else if (dataType==DataTypes.TYPE_FLOAT) {
                    return "float";
                } else if (dataType==DataTypes.TYPE_DOUBLE) {
                    return "double";
                } else if (dataType==DataTypes.TYPE_STRING) {
                    return "string";
                } else if (dataType==DataTypes.TYPE_BOOLEAN) {
                    return "boolean";
                } else if (dataType==DataTypes.TYPE_JSON) {
                    return "json";
                } else if (dataType==DataTypes.TYPE_MEDIUMTEXT) {
                    return "mediumtext";
                } else if (dataType==DataTypes.TYPE_TEXT) {
                    return "text";
                } else if (dataType==DataTypes.TYPE_LONGTEXT) {
                    return "longtext";
                } else if (dataType==DataTypes.TYPE_DOCUMENT) {
                    return "document";
                } else if (dataType==DataTypes.TYPE_LOCATION) {
                    return "location";
                } else if (dataType==DataTypes.TYPE_TIMESTAMP) {
                    return "datetime";
                } else if (dataType==DataTypes.TYPE_TIME) {
                    return "time";
                } else if (dataType==DataTypes.TYPE_DATE) {
                    return "date";
                } else if (dataType==DataTypes.TYPE_ENUM) {
                    return "enum";
                } else if (dataType==DataTypes.TYPE_PAYLOAD) {
                    return "payload";
                } else if (dataType==DataTypes.TYPE_FIXED) {
                    return "fixed";
                } else {
                    return "unknown";
                }            
        }
        
        
        
        public Object convertByDataType(Object input,int dataType,Object format) {
                        
            
                    if (dataType==DataTypes.TYPE_INT) {
                            return Integer.parseInt( (String)input);
                    } else if (dataType==DataTypes.TYPE_LONG) {
                            return Long.parseLong( (String)input);
                    } else if (dataType==DataTypes.TYPE_FLOAT) {
                            return Float.parseFloat( (String)input);
                    } else if (dataType==DataTypes.TYPE_DOUBLE) {
                            return Double.parseDouble( (String)input);
                    } else if (dataType==DataTypes.TYPE_STRING) {
                            return (String) input;
                    } else if (dataType==DataTypes.TYPE_JSON) {
                            return (String) input;
                    } else if (dataType==DataTypes.TYPE_DOCUMENT) {
                            return (String) input;
                    } else if (dataType==DataTypes.TYPE_LOCATION) {
                            return (String) input;
                    } else if (dataType==DataTypes.TYPE_TIMESTAMP) {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern((String)format);
                            LocalDateTime localDateTime = LocalDate.parse((String)input, formatter).atStartOfDay();
                            return Timestamp.valueOf(localDateTime);
                    } else if (dataType==DataTypes.TYPE_DATETIME) {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern((String)format);
                            LocalDateTime localDateTime = LocalDate.parse((String)input, formatter).atStartOfDay();
                            return Timestamp.valueOf(localDateTime);
                    } else if (dataType==DataTypes.TYPE_DATE) {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern((String)format);
                            LocalDateTime localDateTime = LocalDate.parse((String)input, formatter).atStartOfDay();
                            return Date.valueOf(localDateTime.toLocalDate());
                    } else if (dataType==DataTypes.TYPE_TIME) {
                            return Time.valueOf((String)input);
                    } else if (dataType==DataTypes.TYPE_BOOLEAN) {
                            String v = (String)input;
                            if (v.equals("on") || (v.equals("true") || (v.equals("active")))) {
                                return 1;
                            } else {
                                return 0;
                            }

                    } else {
                        return null;
                    }           
                    
        }
    
}
