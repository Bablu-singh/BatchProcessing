package com.batch.demo.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import java.util.concurrent.ScheduledFuture;

@Service
public class ChunkProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(ChunkProcessingService.class);
    private static final int CHUNK_SIZE = 1000;

    private final JdbcTemplate jdbcTemplate;

    private final ThreadPoolTaskScheduler taskScheduler;

    private ScheduledFuture<?> scheduledFuture;

    public ChunkProcessingService(JdbcTemplate jdbcTemplate, ThreadPoolTaskScheduler taskScheduler) {
        this.jdbcTemplate = jdbcTemplate;
        this.taskScheduler = taskScheduler;
    }

    public void startProcessing() {
        if (scheduledFuture == null || scheduledFuture.isCancelled()) {
            scheduledFuture = taskScheduler.schedule(this::processChunk, new CronTrigger("0 * * * * ?"));
            logger.info("Scheduled task started.");
        } else {
            logger.info("Scheduled task is already running.");
        }
        StateStorage.setRunning(true);
    }

    public void stopProcessing() {
        if (scheduledFuture != null) {
            boolean cancelResult = scheduledFuture.cancel(true);
            logger.info("Scheduled task cancelled: {}", cancelResult);
        } else {
            logger.info("No scheduled task to cancel.");
        }
        StateStorage.reset();
        logger.info("State storage reset. Processing stopped.");
    }

    public void processChunk() {
        logger.info("processChunk method called.");

        if (!StateStorage.isRunning()) {
            logger.info("No job is running. Exiting processChunk.");
            return;
        }

        String carrier = StateStorage.getCarrier();
        if (carrier == null) {
            logger.warn("Carrier is not set. Skipping processing.");
            stopProcessing();
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            if (!StateStorage.isUpdateCompleted()) {
                int updatedRows = updateRecords(carrier);
                if (updatedRows == 0) {
                    logger.info("No records to process for update. Moving to insert.");
                    StateStorage.setUpdateCompleted(true);
                } else {
                    logger.info("Processed {} records in the update step.", updatedRows);
                }
            } else if (!StateStorage.isInsertCompleted()) {
                int insertedRows = insertRecords();
                logger.info("Inserted {} records into table1_history.", insertedRows);
                StateStorage.setInsertCompleted(true);
            } else if (!StateStorage.isTable2UpdateCompleted()) {
                int updatedRowsInTable2 = updateTable2Records(carrier);
                if (updatedRowsInTable2 == 0) {
                    logger.info("No records to process for table2 update. Moving to insert into table2_history.");
                    StateStorage.setTable2UpdateCompleted(true);
                } else {
                    logger.info("Processed {} records in the table2 update step.", updatedRowsInTable2);
                }
            } else {
                int insertedRowsInTable2History = insertIntoTable2History();
                logger.info("Inserted {} records into table2_history.", insertedRowsInTable2History);
                stopProcessing();
            }
        } catch (Exception e) {
            logger.error("Error occurred during processing", e);
            stopProcessing();
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            if (duration > 60000) {
                logger.info("Chunk processing took longer than a minute. Skipping next scheduled run.");
            } else {
                logger.info("Waiting for the next scheduled run.");
            }
        }
    }

    private int updateRecords(String carrier) {
        String updateQuery = "UPDATE table1 SET status ='I', programname='sww' WHERE carrier = ? AND status!='I' AND groupID IN (SELECT groupID FROM table2 WHERE status='F' AND carrier = ?) LIMIT " + CHUNK_SIZE;
        int rows = jdbcTemplate.update(updateQuery, carrier, carrier);
        logger.debug("Updated {} records in database for carrier: {}", rows, carrier);
        return rows;
    }

    private int insertRecords() {
        String insertQuery = "INSERT INTO table1history (SELECT * FROM table1 WHERE programname='sww')";
        int rows = jdbcTemplate.update(insertQuery);
        logger.debug("Inserted {} records into table1_history.", rows);
        return rows;
    }

    private int updateTable2Records(String carrier) {
        String updateQuery = "UPDATE table2 SET status ='I', programname='sww' WHERE carrier = ? AND status!='I' AND groupID IN (SELECT groupID FROM table1 WHERE status='T' AND carrier = ?) LIMIT " + CHUNK_SIZE;
        int rows = jdbcTemplate.update(updateQuery, carrier, carrier);
        logger.debug("Updated {} records in table2 for carrier: {}", rows, carrier);
        return rows;
    }

    private int insertIntoTable2History() {
        String insertQuery = "INSERT INTO table2history (SELECT * FROM table2 WHERE programname='sww')";
        int rows = jdbcTemplate.update(insertQuery);
        logger.debug("Inserted {} records into table2_history.", rows);
        return rows;
    }
}
