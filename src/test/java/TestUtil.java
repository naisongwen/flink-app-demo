import com.dlink.health.common.AlertUtil;
import com.dlink.health.common.StringUtil;
import org.eclipse.jetty.util.ajax.JSON;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class TestUtil {
    @Test
    public void testEncode() throws IOException {
        String value = "{\"last_stp:auth_info\": {\"last_\":\"step\":\"authen\":\"\":ticat:\"ion\"}}";
        value="{\"a\":\"\u0000、、\\ffff\"}";
        // String value="{\"owner\":\"dbo\",\"columnNum\":\"64\",\"rowNum\":\"1\",\"operationType\":\"I\",\"tableName\":\"NcovLimsurvey\",\"scn\":16024370427765260292,\"opTs\":\"2022-04-25 18:23:55\",\"loaderTime\":\"2022-04-25 18:23:59\",\"trainid\":247415144,\"DBNAME\":\"ESURVEY\",\"rowid\":\"ARNYTRAAAAAEdvNAAJ\",\"load_seq\":122601,\"afterColumnList\":{\"ResultOID\":\"0baec98549a64faa854ef5a1071aa2b8\",\"Badge\":\"E00790\",\"Eid\":940,\"IsValid\":1,\"CreateBy\":\"940\",\"CreateDate\":\"2022-04-25 18:23:55.600\",\"UpdateBy\":\"940\",\"UpdateDate\":\"2022-04-25 18:23:55.600\",\"ShowName\":\"E00790-Bing Wang\",\"Q1\":\"\",\"Q2\":null,\"Q3\":null,\"Q4\":null,\"SubmissionDate\":\"2022-04-25 18:23:55\",\"Q5\":null,\"Q6\":null,\"Q7\":null,\"Q8\":null,\"Q9\":null,\"Q10\":null,\"Q11\":null,\"Q19\":null,\"Q20\":\"无以上区域旅居行为\",\"Q21\":null,\"Q22\":null,\"Q23\":null,\"Q24\":null,\"Q25\":null,\"Q26\":\"否\",\"Q27\":null,\"Q28\":null,\"Q29\":null,\"Q30\":null,\"Q31\":null,\"Q32\":null,\"EmpHealthStatus\":\"green\",\"TotalEmpHealthStatus\":null,\"Q33\":null,\"Q34\":null,\"Q35\":null,\"Q36\":null,\"Q37\":null,\"Q38\":null,\"Q39\":null,\"Q40\":null,\"Q41\":null,\"Q42\":null,\"Q43\":\"否\",\"Q44\":null,\"Q45\":\"同意\",\"Q46\":\"\",\"Q47\":\"\",\"Q48\":\"\",\"Q49\":\"\",\"Q50\":\"\",\"Q51\":\"否^\",\"Q52\":null}}";
        value = StringUtil.formatErrorJson(value);
        byte[] b = value.getBytes(StandardCharsets.UTF_8);
        String content = new String(b, "utf-8");
        Map<String, Object> obj = (Map<String, Object>) JSON.parse(content);
        System.out.println(obj);
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode jsonNode = mapper.readTree(b);
//        new JsonNodeDeserializationSchema().deserialize(b);
    }

    @Test
    public void testUtil() {
        Map obj = (Map) JSON.parse("{\"status\":\"ok\"}");
        assert (obj.get("status").equals("ok"));
    }


    @Test
    public void testDate() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDate = LocalDateTime.parse("2022-04-25 20:36:00", dtf);
        String date = localDate.format(dtf);
        System.out.println(date);
    }

    @Test
    public void testSend() throws IOException {
        AlertUtil.sendWX("wwd74cbc76ba5ca757", "ERpYdvkWBgzW1bwxCVDxDGYiWoSnuuXqA2oSHGUSNtw", "e01092", "测试消息");
    }
}
