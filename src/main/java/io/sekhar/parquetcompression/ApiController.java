package io.sekhar.parquetcompression;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

//@RestController
public class ApiController {

    private final ParquetCompresser parquetCompresser;

    public ApiController(ParquetCompresser parquetCompresser) {
        this.parquetCompresser = parquetCompresser;
    }

//    @PostMapping("/api/v1/compress")
    public void compress() throws IOException {
        var filePath = "src/main/resources/uncompressed-sql-results.csv";
        var compressionType = CompressionCodecName.GZIP;
        parquetCompresser.compressCsv(filePath, compressionType);
    }
}
