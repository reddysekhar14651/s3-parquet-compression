package io.sekhar.parquetcompression;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class ParquetCompressionApplication {

    public static void main(String[] args) throws IOException {
        ParquetCompresser parquetCompresser = new ParquetCompresser();
        var filePath = "src/main/resources/uncompressed-sql-results.csv";
        var compressionType = CompressionCodecName.SNAPPY;
        parquetCompresser.compressCsv(filePath, compressionType);
    }

}
