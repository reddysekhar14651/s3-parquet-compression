package io.sekhar.parquetcompression;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.List;

public class CustomWriteSupport extends WriteSupport<List<String>> {
    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> cols;

    CustomWriteSupport(MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteSupport.WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(List<String> values) {
        if (values.size() != cols.size()) {
            throw new ParquetEncodingException("Invalid input data. Expecting " +
                    cols.size() + " columns. Input had " + values.size() + " columns (" + cols + ") : " + values);
        }

        recordConsumer.startMessage();
        for (int i = 0; i < cols.size(); ++i) {
            String val = values.get(i);
            // val.length() == 0 indicates a NULL value.
            if (val.length() > 0) {
                recordConsumer.startField(cols.get(i).getPath()[0], i);
                switch (cols.get(i).getType()) {
                    case BOOLEAN -> recordConsumer.addBoolean(Boolean.parseBoolean(val));
                    case FLOAT -> recordConsumer.addFloat(Float.parseFloat(val));
                    case DOUBLE -> recordConsumer.addDouble(Double.parseDouble(val));
                    case INT32 -> recordConsumer.addInteger(Integer.parseInt(val));
                    case INT64 -> recordConsumer.addLong(Long.parseLong(val));
                    case BINARY -> recordConsumer.addBinary(stringToBinary(val));
                    default -> throw new ParquetEncodingException("Unsupported column type: " + cols.get(i).getType());
                }
                recordConsumer.endField(cols.get(i).getPath()[0], i);
            }
        }
        recordConsumer.endMessage();
    }

    private Binary stringToBinary(Object value) {
        return Binary.fromString(value.toString());
    }
}

