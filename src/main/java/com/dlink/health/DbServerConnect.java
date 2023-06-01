package com.dlink.health;

import com.dlink.health.common.SysEmployee;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DbServerConnect {
    //jdbc:postgresql://10.201.0.212:5432/postgres postgres postgres company
    public static void main(String[] args) throws Exception {
        String sqlServerUrl=args[0];
        String sqlServerUserName=args[1];
        String sqlServerPasswd=args[2];
        String sql;
        if(args.length>3)
         sql=args[3];
        else
            sql = "select eid,Name,Badge from SysEmployee where EmpStatus='1'";
        Connection connection = DriverManager.getConnection(sqlServerUrl, sqlServerUserName,sqlServerPasswd);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            SysEmployee employee = new SysEmployee();
            int eid=resultSet.getInt(1);
            employee.setEid(eid);
            String name=resultSet.getString(2);
            employee.setName(name);
            String badge=resultSet.getString(3);
            employee.setBadge(badge);
            System.out.println(employee);
        }
    }
}
