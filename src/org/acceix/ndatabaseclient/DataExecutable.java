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
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author zrid
 */
public class DataExecutable {
    
    private PreparedStatement preparedStatement;
    private DataConnector dataConnector;
    public static final int UPDATE=1,DELETE=2,SELECT=3,INSERT=4,UNKNOWN=0;
    private String query;
    private int sqlcommand;

 

    public DataExecutable() {
    }

    public DataExecutable(PreparedStatement preparedStatement,DataConnector connection, int sqlcommand_t) {
        this.preparedStatement = preparedStatement;
        this.dataConnector = connection;
        this.sqlcommand = sqlcommand_t;
    }
    

    public void setPreparedStatement(PreparedStatement preparedStatement,DataConnector connection) {
        this.preparedStatement = preparedStatement;
        this.dataConnector = connection;
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
    
    
    public int execute() throws SQLException, ClassNotFoundException {
                int res = preparedStatement.executeUpdate();
                preparedStatement.close();
                if (!dataConnector.isUseSameConnection()) {
                    dataConnector.getConnection().close();
                }
                return res;
    }   
    
    public int executeAndGetID() throws SQLException, ClassNotFoundException {

            preparedStatement.executeUpdate();

            ResultSet rs = preparedStatement.getGeneratedKeys();
            int insertedId;
            if (rs.next()) {
                insertedId = rs.getInt(1);
            } else {
                insertedId = -1;
            }   

            preparedStatement.close();
            if (!dataConnector.isUseSameConnection()) {
                dataConnector.getConnection().close();
            }
            return insertedId;


    }       
    
    public MachineDataSet executeSelect() throws SQLException, ClassNotFoundException {

                MachineDataSet resultDataset =  new ResultSetConverter().resultSetToMachineDataSet(preparedStatement.executeQuery());
                
                preparedStatement.close();
                
                if (!dataConnector.isUseSameConnection()) {
                    dataConnector.getConnection().close();
                }
                
                return resultDataset;
                
    }    
    
}
