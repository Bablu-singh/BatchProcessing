package com.batch.demo.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import java.util.concurrent.ScheduledFuture;

@Service
public class ChunkProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(ChunkProcessingService.class);
    private static final int CHUNK_SIZE = 1000;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ThreadPoolTaskScheduler taskScheduler;

    private ScheduledFuture<?> scheduledFuture;

    public void startProcessing() {
        if (scheduledFuture == null || scheduledFuture.isCancelled()) {
            scheduledFuture = taskScheduler.schedule(this::processChunk, new CronTrigger("0 * * * * *"));
        }
    }

    public void stopProcessing() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        CarrierStorage.reset();
        logger.info("Processing stopped.");
    }

    @Scheduled(cron = "0 * * * * ?")  // This schedules the job to run at the top of every minute
    public void processChunk() {
        if (CarrierStorage.isRunning()) {
            logger.info("Previous chunk is still being processed. Skipping this run.");
            return;
        }

        String carrier = CarrierStorage.getCarrier();
        if (carrier == null) {
            logger.warn("Carrier is not set. Skipping processing.");
            stopProcessing();  // Stop the scheduler if carrier is not set
            return;
        }

        CarrierStorage.setRunning(true);
        long startTime = System.currentTimeMillis();

        try {
            if (!CarrierStorage.isUpdateCompleted()) {
                int updatedRows = updateRecords(carrier);
                if (updatedRows == 0) {
                    logger.info("No records to process for update. Moving to insert.");
                    CarrierStorage.setUpdateCompleted(true);
                } else {
                    logger.info("Processed {} records in the update step.", updatedRows);
                }
            } else if (!CarrierStorage.isInsertCompleted()) {
                int insertedRows = insertRecords();
                logger.info("Inserted {} records into table1_history.", insertedRows);
                CarrierStorage.setInsertCompleted(true);
            } else if (!CarrierStorage.isTable2UpdateCompleted()) {
                int updatedRowsInTable2 = updateTable2Records(carrier);
                if (updatedRowsInTable2 == 0) {
                    logger.info("No records to process for table2 update. Moving to insert into table2_history.");
                    CarrierStorage.setTable2UpdateCompleted(true);
                } else {
                    logger.info("Processed {} records in the table2 update step.", updatedRowsInTable2);
                }
            } else {
                int insertedRowsInTable2History = insertIntoTable2History();
                logger.info("Inserted {} records into table2_history.", insertedRowsInTable2History);
                stopProcessing(); // Stop processing after completing all tasks
            }
        } catch (Exception e) {
            logger.error("Error occurred during processing", e);
            stopProcessing(); // Stop processing on error
        } finally {
            CarrierStorage.setRunning(false);
            long duration = System.currentTimeMillis() - startTime;
            if (duration > 60000) {
                logger.info("Chunk processing took longer than a minute. Skipping next scheduled run.");
            } else {
                logger.info("Waiting for the next scheduled run.");
            }
        }
    }

    private int updateRecords(String carrier) {
        String updateQuery = "UPDATE table1 SET status ='I', programname='sww' WHERE carrier = ? AND groupID IN (SELECT groupID FROM table1 WHERE status='F' AND carrier = ?) LIMIT " + CHUNK_SIZE;
        int rows = jdbcTemplate.update(updateQuery, carrier, carrier);
        logger.debug("Updated {} records in database for carrier: {}", rows, carrier);
        return rows;
    }

    private int insertRecords() {
        String insertQuery = "INSERT INTO table1_history (SELECT * FROM table1 WHERE programname='sww')";
        int rows = jdbcTemplate.update(insertQuery);
        logger.debug("Inserted {} records into table1_history.", rows);
        return rows;
    }

    private int updateTable2Records(String carrier) {
        String updateQuery = "UPDATE table2 SET status ='I', programname='sww' WHERE carrier = ? AND groupID IN (SELECT groupID FROM table1 WHERE status='T' AND carrier = ?) LIMIT " + CHUNK_SIZE;
        int rows = jdbcTemplate.update(updateQuery, carrier, carrier);
        logger.debug("Updated {} records in table2 for carrier: {}", rows, carrier);
        return rows;
    }

    private int insertIntoTable2History() {
        String insertQuery = "INSERT INTO table2_history (SELECT * FROM table1 WHERE programname='sww')";
        int rows = jdbcTemplate.update(insertQuery);
        logger.debug("Inserted {} records into table2_history.", rows);
        return rows;
    }
}
