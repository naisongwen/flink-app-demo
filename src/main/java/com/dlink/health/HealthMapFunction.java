package com.dlink.health;

import com.dlink.health.common.AlertUtil;
import com.dlink.health.common.PersonStat;
import com.dlink.health.common.SysEmployee;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class HealthMapFunction extends RichFlatMapFunction<Row,Row> {

    private final String codeType;
    private final String toUsers;
    String corpid;
    String corpsecret;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String passwd;
    private final Integer reloadInterval;
    protected Map<String, PersonStat> personStatConcurrentHashMap = new ConcurrentHashMap<>();


    public HealthMapFunction(String codeType, String corpid, String corpsecret, String toUsers, String jdbcUrl, String jdbcUser, String passwd, Integer reloadInterval) {
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
                    Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, passwd);
                    String sql = "select personcardno,vname from v_vendorpersonstatistical where CA_STATE in ('Active','New')";
                    PreparedStatement preparedStatement = connection.prepareStatement(sql);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        PersonStat personStat = new PersonStat();
                        String personcardno=resultSet.getString(1);
                        personStat.setPersoncardno(personcardno);
                        String vname=resultSet.getString(2);
                        personStat.setVname(vname);
                        personStatConcurrentHashMap.put(personStat.getPersoncardno(), personStat);
                    }
                    connection.close();
                } catch (SQLException e) {
                    log.error(String.format("exception occur when loading v_vendorpersonstatistical data with jdbcUrl:%s,jdbcUser:%s,passwd:%s",jdbcUrl,jdbcUser,passwd),e);
                }
            }
        };
        executorService.scheduleAtFixedRate(timerTask, 0, reloadInterval, TimeUnit.HOURS);
    }

    @Override
    public void flatMap(Row row, Collector<Row> out) throws Exception {
        String msg;
        LocalDateTime createDate = (LocalDateTime) row.getField(0);
        String Q1 = (String) row.getField(2);
        String Q3 = (String) row.getField(3);
        String Q74 = (String) row.getField(4);
        if (personStatConcurrentHashMap.containsKey(Q1)) {
            Row resultRow = new Row(6);
            PersonStat personStat = personStatConcurrentHashMap.get(Q1);
            String vname = personStat.getVname();
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String date = createDate.format(timeFormatter);
            msg = String.format("%s(%s)%s行动轨迹与目前阳性感染者详细活动轨迹有重合:%s",vname,Q1,date, Q74);
            AlertUtil.sendWX(corpid, corpsecret, toUsers, msg);
            resultRow.setField(2, vname);
            resultRow.setField(0, Q1);
            resultRow.setField(1, Q3);
            resultRow.setField(3, codeType);
            resultRow.setField(4, Q74);
            resultRow.setField(5, createDate.toLocalDate());
            out.collect(resultRow);
        }
    }
}
