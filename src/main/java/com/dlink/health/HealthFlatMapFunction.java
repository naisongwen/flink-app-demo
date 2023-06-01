package com.dlink.health;

import com.dlink.health.common.AlertUtil;
import com.dlink.health.common.SysEmployee;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HealthFlatMapFunction extends RichFlatMapFunction<Row, Row> {

    private final String codeType;
    private final String toUsers;
    private final String jdbcUser;
    private final String jdbcUrl;
    private final String passwd;
    private final Integer reloadInterval;
    protected Map employeeHashMap = new ConcurrentHashMap<Integer, SysEmployee>();
    String corpid;
    String corpsecret;

    public HealthFlatMapFunction(String codeType, String corpid, String corpsecret, String toUsers, String jdbcUrl, String jdbcUser, String passwd, Integer reloadInterval) {
        this.codeType = codeType;
        this.corpid = corpid;
        this.corpsecret = corpsecret;
        this.toUsers = toUsers;
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.passwd = passwd;
        this.reloadInterval = reloadInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    //https://techcommunity.microsoft.com/t5/azure-database-support-blog/pkix-path-building-failed-unable-to-find-valid-certification/ba-p/2591304
                    Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, passwd);
                    String sql = "select eid,Name,Badge from SysEmployee where EmpStatus='1'";
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
                        employeeHashMap.put(employee.getEid(), employee);
                    }
                    connection.close();
                } catch (SQLException e) {
                    log.error(String.format("exception occur when loading employee data with jdbcUrl:%s,jdbcUser:%s,passwd:%s",jdbcUrl,jdbcUser,passwd),e);
                }
            }
        };
        executorService.scheduleAtFixedRate(timerTask, 0, reloadInterval, TimeUnit.HOURS);
    }

    @Override
    public void flatMap(Row row, Collector<Row> out) throws Exception {
        String msg;
        int eid = (int) row.getField(0);
        String status= (String) row.getField(1);
        LocalDateTime createDate= (LocalDateTime) row.getField(2);
        DateTimeFormatter timeFormatter=DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String date=createDate.format(timeFormatter);
        String badge =null;
        String name =null;
        if (employeeHashMap.containsKey(eid)) {
            SysEmployee employee = (SysEmployee) employeeHashMap.get(eid);
            badge=employee.getBadge();
            name=employee.getName();
            if(codeType.equals("防疫问卷信息")){
                msg=String.format("%s(%s)%s行动轨迹与目前阳性感染者详细活动轨迹有重合:%s",employee.getName(),employee.getBadge(),date,row.getField(4));
            }else {
                msg = String.format("%s(%s)%s%s为%s", employee.getName(), employee.getBadge(), date, codeType, (status.equals("3") ? "红色" : "黄色"));
            }
        }else{
            if(codeType.equals("防疫问卷信息")){
                msg=String.format("%d %s 行动轨迹与目前阳性感染者详细活动轨迹有重合:%s",eid,date,row.getField(4));
            }else {
                msg = String.format("%d %s %s为%s", eid, date, codeType, (status.equals("3") ? "红色" : "黄色"));
            }
        }
        AlertUtil.sendWX(corpid,corpsecret,toUsers,msg);
        Row resultRow=new Row(5);
        resultRow.setField(0,badge);
        resultRow.setField(1,name);
        resultRow.setField(2,codeType);
        resultRow.setField(3,status);
        resultRow.setField(4,createDate.toLocalDate());
        out.collect(resultRow);
    }
}
