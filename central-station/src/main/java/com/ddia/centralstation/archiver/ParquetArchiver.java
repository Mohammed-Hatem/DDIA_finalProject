package com.ddia.centralstation.archiver;

import com.ddia.centralstation.model.WeatherStatusMessage;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Archives all weather status messages to Parquet files.
 * Records are batched (default 10K) and written in partitions by date and station ID.
 */
@Component
public class ParquetArchiver {

    @Value("${parquet.directory:./data/parquet}")
    private String parquetDir;

    @Value("${parquet.batch.size:10000}")
    private int batchSize;

    private final List<WeatherStatusMessage> buffer = new ArrayList<>();
    private final ReentrantLock bufferLock = new ReentrantLock();

    private static final Schema SCHEMA = SchemaBuilder.record("WeatherStatus")
            .namespace("com.ddia")
            .fields()
            .requiredLong("station_id")
            .requiredLong("s_no")
            .requiredString("battery_status")
            .requiredLong("status_timestamp")
            .requiredBoolean("message_dropped")
            .requiredInt("humidity")
            .requiredInt("temperature")
            .requiredInt("wind_speed")
            .endRecord();

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

    @PostConstruct
    public void init() {
        new File(parquetDir).mkdirs();
        System.out.println("[ParquetArchiver] Initialized — dir=" + parquetDir
                + " batchSize=" + batchSize);
    }

    @PreDestroy
    public void flushOnShutdown() {
        flush();
    }

    /**
     * Add a message to the buffer. When the buffer reaches batchSize, flush to a Parquet file.
     */
    public void archive(WeatherStatusMessage message) {
        List<WeatherStatusMessage> batchToWrite = null;
        bufferLock.lock();
        try {
            buffer.add(message);
            if (buffer.size() >= batchSize) {
                batchToWrite = new ArrayList<>(buffer);
                buffer.clear();
            }
        } finally {
            bufferLock.unlock();
        }

        if (batchToWrite != null) {
            writeBatch(batchToWrite);
        }
    }

    /** Force flush any remaining records in the buffer. */
    public void flush() {
        List<WeatherStatusMessage> batchToWrite = null;
        bufferLock.lock();
        try {
            if (!buffer.isEmpty()) {
                batchToWrite = new ArrayList<>(buffer);
                buffer.clear();
            }
        } finally {
            bufferLock.unlock();
        }

        if (batchToWrite != null) {
            writeBatch(batchToWrite);
        }
    }

    private void writeBatch(List<WeatherStatusMessage> batch) {
        if (batch.isEmpty()) return;

        // Partition by message time (date) and station ID:
        // parquetDir/date=YYYY-MM-DD/station_id=<id>/batch_<timestamp>.parquet
        record GroupKey(String date, long stationId) {}

        var groups = batch.stream().collect(java.util.stream.Collectors.groupingBy(msg -> {
            String date = DATE_FMT.format(Instant.ofEpochSecond(msg.getStatusTimestamp()));
            return new GroupKey(date, msg.getStationId());
        }));

        for (var entry : groups.entrySet()) {
            GroupKey key = entry.getKey();
            List<WeatherStatusMessage> records = entry.getValue();

            String partitionDir = parquetDir
                    + File.separator + "date=" + key.date()
                    + File.separator + "station_id=" + key.stationId();
            new File(partitionDir).mkdirs();

            String fileName = partitionDir + File.separator + "batch_" + System.currentTimeMillis() + ".parquet";
            Path path = new Path(fileName);

            try {
                Configuration conf = new Configuration();
                try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                        .withSchema(SCHEMA)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withConf(conf)
                        .build()) {

                    for (WeatherStatusMessage msg : records) {
                        GenericRecord record = new GenericData.Record(SCHEMA);
                        record.put("station_id", msg.getStationId());
                        record.put("s_no", msg.getSNo());
                        record.put("battery_status",
                                msg.getBatteryStatus() != null ? msg.getBatteryStatus() : "");
                        record.put("status_timestamp", msg.getStatusTimestamp());
                        record.put("message_dropped", msg.isMessageDropped());
                        if (msg.getWeather() != null) {
                            record.put("humidity", msg.getWeather().getHumidity());
                            record.put("temperature", msg.getWeather().getTemperature());
                            record.put("wind_speed", msg.getWeather().getWindSpeed());
                        } else {
                            record.put("humidity", 0);
                            record.put("temperature", 0);
                            record.put("wind_speed", 0);
                        }
                        writer.write(record);
                    }
                }

                System.out.println("[ParquetArchiver] Wrote " + records.size()
                        + " records to " + fileName);
            } catch (IOException e) {
                System.err.println("[ParquetArchiver] Error writing batch: " + e.getMessage());
            }
        }
    }
}
