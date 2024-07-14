package com.batch.demo.config;

public class StateStorage {
    private static String carrier;
    private static boolean isRunning = false;
    private static boolean updateCompleted = false;
    private static boolean insertCompleted = false;
    private static boolean table2UpdateCompleted = false;

    public static synchronized String getCarrier() {
        return carrier;
    }

    public static synchronized void setCarrier(String carrier) {
        StateStorage.carrier = carrier;
    }

    public static synchronized void clearCarrier() {
        StateStorage.carrier = null;
    }

    public static synchronized boolean isRunning() {
        return isRunning;
    }

    public static synchronized void setRunning(boolean running) {
        isRunning = running;
    }

    public static synchronized boolean isUpdateCompleted() {
        return updateCompleted;
    }

    public static synchronized void setUpdateCompleted(boolean updateCompleted) {
        StateStorage.updateCompleted = updateCompleted;
    }

    public static synchronized boolean isInsertCompleted() {
        return insertCompleted;
    }

    public static synchronized void setInsertCompleted(boolean insertCompleted) {
        StateStorage.insertCompleted = insertCompleted;
    }

    public static synchronized boolean isTable2UpdateCompleted() {
        return table2UpdateCompleted;
    }

    public static synchronized void setTable2UpdateCompleted(boolean table2UpdateCompleted) {
        StateStorage.table2UpdateCompleted = table2UpdateCompleted;
    }

    public static synchronized void reset() {
        carrier = null;
        isRunning = false;
        updateCompleted = false;
        insertCompleted = false;
        table2UpdateCompleted = false;
    }
}
