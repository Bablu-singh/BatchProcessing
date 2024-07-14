package com.batch.demo.config;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import java.util.concurrent.ScheduledFuture;

@Service
@Slf4j
public class ChunkProcessingService {

    private static final int CHUNK_SIZE = 1000;

    private final JdbcTemplate jdbcTemplate;

    private final ThreadPoolTaskScheduler taskScheduler;

    private ScheduledFuture<?> scheduledFuture;

    public ChunkProcessingService(JdbcTemplate jdbcTemplate, @Qualifier("taskScheduler1") ThreadPoolTaskScheduler taskScheduler) {
        this.jdbcTemplate = jdbcTemplate;
        this.taskScheduler = taskScheduler;
    }

    public void startProcessing() {
        if (scheduledFuture == null || scheduledFuture.isCancelled()) {
            scheduledFuture = taskScheduler.schedule(this::processChunk, new CronTrigger("0 * * * * ?"));
            log.info("Scheduled task started.");
        } else {
            log.info("Scheduled task is already running.");
        }
        StateStorage.setRunning(true);
    }

    public void stopProcessing() {
        if (scheduledFuture != null) {
            boolean cancelResult = scheduledFuture.cancel(true);
            log.info("Scheduled task cancelled: {}", cancelResult);
        } else {
            log.info("No scheduled task to cancel.");
        }
        StateStorage.reset();
        log.info("State storage reset. Processing stopped.");
    }

    public void processChunk() {
        log.info("processChunk method called.");

        if (!StateStorage.isRunning()) {
            log.info("No job is running. Exiting processChunk.");
            return;
        }

        String carrier = StateStorage.getCarrier();
        if (carrier == null) {
            log.warn("Carrier is not set. Skipping processing.");
            stopProcessing();
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            if (!StateStorage.isUpdateCompleted()) {
                int updatedRows = updateRecords(carrier);
                if (updatedRows == 0) {
                    log.info("No records to process for update. Moving to insert.");
                    StateStorage.setUpdateCompleted(true);
                } else {
                    log.info("Processed {} records in the update step.", updatedRows);
                }
            } else if (!StateStorage.isInsertCompleted()) {
                int insertedRows = insertRecords();
                log.info("Inserted {} records into table1_history.", insertedRows);
                StateStorage.setInsertCompleted(true);
            } else if (!StateStorage.isTable2UpdateCompleted()) {
                int updatedRowsInTable2 = updateTable2Records(carrier);
                if (updatedRowsInTable2 == 0) {
                    log.info("No records to process for table2 update. Moving to insert into table2_history.");
                    StateStorage.setTable2UpdateCompleted(true);
                } else {
                    log.info("Processed {} records in the table2 update step.", updatedRowsInTable2);
                }
            } else {
                int insertedRowsInTable2History = insertIntoTable2History();
                log.info("Inserted {} records into table2_history.", insertedRowsInTable2History);
                stopProcessing();
            }
        } catch (Exception e) {
            log.error("Error occurred during processing", e);
            stopProcessing();
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            if (duration > 60000) {
                log.info("Chunk processing took longer than a minute. Skipping next scheduled run.");
            } else {
                log.info("Waiting for the next scheduled run.");
            }
        }
    }

    private int updateRecords(String carrier) {
        String updateQuery = "UPDATE table1 SET status ='I', programname='sww' WHERE carrier = ? AND status!='I' AND groupID IN (SELECT groupID FROM table2 WHERE status='F' AND carrier = ?) LIMIT " + CHUNK_SIZE;
        int rows = jdbcTemplate.update(updateQuery, carrier, carrier);
        log.debug("Updated {} records in database for carrier: {}", rows, carrier);
        return rows;
    }

    private int insertRecords() {
        String insertQuery = "INSERT INTO table1history (SELECT * FROM table1 WHERE programname='sww')";
        int rows = jdbcTemplate.update(insertQuery);
        log.debug("Inserted {} records into table1_history.", rows);
        return rows;
    }

    private int updateTable2Records(String carrier) {
        String updateQuery = "UPDATE table2 SET status ='I', programname='sww' WHERE carrier = ? AND status!='I' AND groupID IN (SELECT groupID FROM table1 WHERE status='T' AND carrier = ?) LIMIT " + CHUNK_SIZE;
        int rows = jdbcTemplate.update(updateQuery, carrier, carrier);
        log.debug("Updated {} records in table2 for carrier: {}", rows, carrier);
        return rows;
    }

    private int insertIntoTable2History() {
        String insertQuery = "INSERT INTO table2history (SELECT * FROM table2 WHERE programname='sww')";
        int rows = jdbcTemplate.update(insertQuery);
        log.debug("Inserted {} records into table2_history.", rows);
        return rows;
    }
}
