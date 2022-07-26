/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.acceix.ndatabaseclient;

import java.util.LinkedHashMap;

/**
 *
 * @author zrid
 */
public class DataTable {
    
    
        private final LinkedHashMap<String,Boolean> tableNames;
        private String lastTable = null;
        private String previosTable = null;
        private String firstTable = null;
        private final DataConnector nDataConnector;





        public String[] getTableNames() {
            
            String[] l_l = new String[tableNames.size()];
            int i=0;
            for (Object s : tableNames.keySet().toArray()) {
                
                l_l[i] = (String)s;
                
                i++;
            }
            
            return l_l;
        }

        public DataConnector getnDataConnector() {
            return nDataConnector;
        }

        public void joinTable(String tableName) {
            tableNames.put(tableName,true);
            previosTable = lastTable;            
            lastTable = tableName;
        }

        public void endJoin() {
            lastTable = previosTable;
            previosTable = null;
        }        

        public String getFirstTable() {
            return firstTable;
        }
   
        public String getLastTable() {
            return lastTable;
        }

        public String getPreviosTable() {
            return previosTable;
        }




        public DataTable(String tableName,DataConnector nDataConnector) {
            tableNames = new LinkedHashMap<>();
            tableNames.put(tableName,true);
            firstTable = tableName;
            previosTable = lastTable;
            lastTable = tableName;
            this.nDataConnector = nDataConnector;
        }

        
        public DataSelectable select() {
                return new  DataSelectable(this);
        }
        
        
        public DataInsertable insert() {
                return new DataInsertable(this);
        }
        
        public DataUpdatable update(Integer id) {
                return new DataUpdatable(this,id);
        }        

        public DataUpdatable update() {
                return new DataUpdatable(this);
        }        

        
        public DataDeletable delete(Integer id) {
                return new DataDeletable(this, id);
        }
     
        public DataDeletable delete() {
                return new DataDeletable(this);
        }
        
}
