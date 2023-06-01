import com.dlink.health.common.SysEmployee;
import org.junit.Test;

import java.sql.*;

public class TestMSSqlConnection {

    @Test
    public void testConn() throws SQLException, ClassNotFoundException {
        //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection connection = DriverManager.getConnection("jdbc:sqlserver://10.201.0.212:1433;DatabaseName=testDB;", "sa", "Wm@12345");
        String sql = "select eid,Name,Badge from dbo.SysEmployee";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            SysEmployee employee = new SysEmployee();
            int eid=resultSet.getInt(1);
            String Name=resultSet.getString(2);
            String Badge=resultSet.getString(3);
            System.out.println(employee);
        }
    }
}
