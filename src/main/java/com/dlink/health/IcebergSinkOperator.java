package com.dlink.health;

import com.dlink.health.common.AlertEmploySchema;
import com.dlink.health.common.SysEmployee;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

public class IcebergSinkOperator extends AbstractStreamOperator<Row>
        implements OneInputStreamOperator<SysEmployee,Row> {

    @Override
    public void processElement(StreamRecord<SysEmployee> element) throws Exception {
        SysEmployee employee= element.getValue();
        Row row = new Row(AlertEmploySchema.AlertEmployTableSchema().getFieldCount());
        row.setField(0,employee.getEid());
        row.setField(1,employee.getName());
        row.setField(2,employee.getBadge());
        output.collect(new StreamRecord(row, System.currentTimeMillis()));
    }
}
