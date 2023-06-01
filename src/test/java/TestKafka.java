import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestKafka {

    @Test
    public void testWriteKafka() throws ExecutionException, InterruptedException {
        Properties kafkaProps = new Properties();
        String kafkaServers = "10.201.0.89:9092";
        kafkaProps.put("bootstrap.servers", kafkaServers);
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer",StringSerializer.class.getName());
        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);

        String value="{\"owner\":\"dbo\",\"columnNum\":\"64\",\"rowNum\":\"1\",\"operationType\":\"I\",\"tableName\":\"NcovLimsurvey\",\"scn\":16024370427765260292,\"opTs\":\"2022-04-25 18:23:55\",\"loaderTime\":\"2022-04-25 18:23:59\",\"trainid\":247415144,\"DBNAME\":\"ESURVEY\",\"rowid\":\"ARNYTRAAAAAEdvNAAJ\",\"load_seq\":122601,\"afterColumnList\":{\"ResultOID\":\"0baec98549a64faa854ef5a1071aa2b8\",\"Badge\":\"E00790\",\"Eid\":940,\"IsValid\":1,\"CreateBy\":\"940\",\"CreateDate\":\"2022-04-25 18:23:55.600\",\"UpdateBy\":\"940\",\"UpdateDate\":\"2022-04-25 18:23:55.600\",\"ShowName\":\"E00790-Bing Wang\",\"Q1\":\"\",\"Q2\":null,\"Q3\":null,\"Q4\":null,\"SubmissionDate\":\"2022-04-25 18:23:55\",\"Q5\":null,\"Q6\":null,\"Q7\":null,\"Q8\":null,\"Q9\":null,\"Q10\":null,\"Q11\":null,\"Q19\":null,\"Q20\":\"无以上区域旅居行为\",\"Q21\":null,\"Q22\":null,\"Q23\":null,\"Q24\":null,\"Q25\":null,\"Q26\":\"否\",\"Q27\":null,\"Q28\":null,\"Q29\":null,\"Q30\":null,\"Q31\":null,\"Q32\":null,\"EmpHealthStatus\":\"green\",\"TotalEmpHealthStatus\":null,\"Q33\":null,\"Q34\":null,\"Q35\":null,\"Q36\":null,\"Q37\":null,\"Q38\":null,\"Q39\":null,\"Q40\":null,\"Q41\":null,\"Q42\":null,\"Q43\":\"否\",\"Q44\":null,\"Q45\":\"同意\",\"Q46\":\"\",\"Q47\":\"\",\"Q48\":\"\",\"Q49\":\"\",\"Q50\":\"\",\"Q51\":\"否^\",\"Q52\":null}}";
        value="{\"owner\":\"USER01\",\"tableName\":\" TEST01\",\"operationType\":\"D\",\"rowNum\":\"2\",\"columnNum\":\"10\",\"opTs\":\"2018-02-02 20:21:25\",\"scn\":\"56224332\",\"seqid\":\"1\",\"tranid\":\"5911099065018442\",\"loaderTime\":\"2018-02-02 20:21:29\",\"rowid\":\"AAApMdAAHAAANpfAAH\",\"afterColumnList\":{\"ID\":\"2\",\"NAME\":\"11zjjjj\",\"HDATE\":\"2018-02-01 22:23:48\"}}";
        value="{\"owner\":\"USER01\",\"tableName\":\" TEST01\",\"operationType\":\"U\",\"rowNum\":\"2\",\"columnNum\":\"10\",\"opTs\":\"2018-02-02 20:21:25\",\"scn\":\"56224332\",\"seqid\":\"1\",\"tranid\":\"5911099065018442\",\"loaderTime\":\"2018-02-02 20:21:29\",\"rowid\":\"AAApMdAAHAAANpfAAH\",\"beforeColumnList\":{\"ID\":\"1\",\"NAME\":\"yghzjjjj\",\"HDATE\":\"2018-02-01 22:09:48\"},\"afterColumnList\":{\"ID\":\"2\",\"NAME\":\"11zjjjj\",\"HDATE\":\"2018-02-01 22:23:48\"}}";
        ProducerRecord<String, String> record = new ProducerRecord<>("testTopic5", "testKey",value );//Topic Key Value
        Future future = producer.send(record);
        future.get();
    }
}
