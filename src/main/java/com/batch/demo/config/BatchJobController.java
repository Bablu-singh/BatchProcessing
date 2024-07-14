package com.batch.demo.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/batch")
public class BatchJobController {

    @Autowired
    private ChunkProcessingService chunkProcessingService;

    @GetMapping("/start")
    public ResponseEntity<String> startJob(@RequestParam String carrier) {
        if (CarrierStorage.isRunning()) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Job is already running.");
        }

        try {
            CarrierStorage.reset(); // Reset all states
            CarrierStorage.setCarrier(carrier); // Store the carrier value
            chunkProcessingService.startProcessing();
            return ResponseEntity.ok("Job started successfully");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Job failed to start: " + e.getMessage());
        }
    }
}
