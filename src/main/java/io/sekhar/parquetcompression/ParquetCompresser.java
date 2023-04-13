package io.sekhar.parquetcompression;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class ParquetCompresser {

    private static final String bucketName = "s3-file-compression";

    public void compressCsv(String filePath, CompressionCodecName compressionCodecName) throws IOException {
        CSVParser parser = createCsvParser(filePath);

        // Extracting csv header & csv records
        var headerMap = parser.getHeaderMap();
        var records = parser.getRecords();

        // Extracting schema required for parquet
        var csvSchema = constructSchema(headerMap, records);

        var compressionTypeString = compressionCodecName.toString();

        // create Parquet writer
        Path outputPath = new Path("compressed-" + compressionTypeString + ".parquet");
        MessageType schema = MessageTypeParser.parseMessageType(csvSchema);

        var writer = new CustomParquetWriter(outputPath, schema, true, compressionCodecName);

        long start = Instant.now().toEpochMilli();
        for (CSVRecord csvRecord : records) {
            var listOfValues = new ArrayList<String>();
            System.out.println(csvRecord.toString());
            csvRecord.iterator().forEachRemaining(listOfValues::add);
            writer.write(listOfValues);
        }

        long end = Instant.now().toEpochMilli();
        System.out.println("Time taken for " + compressionTypeString + " is " + (end - start));
        System.out.println("Finished writing");
        writer.close();


        uploadFilesToS3(compressionTypeString, filePath);


    }

    private void uploadFilesToS3(String compressionTypeString, String filePath) {
        var s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(new PutObjectRequest(
                bucketName,
                "uncompressed.csv",
                new File(filePath)
        ));

        s3Client.putObject(new PutObjectRequest(
                bucketName,
                "compressed-"+ compressionTypeString +".parquet",
                new File("compressed-"+ compressionTypeString +".parquet")
        ));
        System.out.println("upload complete");
    }

    private CSVParser createCsvParser(String filePath) throws IOException {
        return CSVFormat.newFormat(',')
                .withQuote('"')
                .withHeader()
                .parse(new FileReader(filePath));
    }

    private String constructSchema(Map<String, Integer> headerMap, List<CSVRecord> records) {
        StringBuilder schemaBuilder = new StringBuilder();
        schemaBuilder.append("message CsvSchema {\n");
        for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
            var fieldName = entry.getKey();
            var fieldType = inferType(records.get(0).get(fieldName));
            schemaBuilder.append("  optional ").append(fieldType).append(" ").append(fieldName).append(";\n");
        }

        schemaBuilder.append("}");

        return schemaBuilder.toString();
    }


    private static String inferType(String value) {
        try {
            Integer.parseInt(value);
            return "int32";
        } catch (NumberFormatException e) {
            try {
                Double.parseDouble(value);
                return "double";
            } catch (NumberFormatException ex) {
                return "binary";
            }
        }
    }
}
