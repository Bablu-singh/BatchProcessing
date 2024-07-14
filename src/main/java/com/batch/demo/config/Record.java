package com.batch.demo.config;

public class Record {
    private String status;
    private String programName;
    private String carrier;
    private Long groupID;

    // Getters and Setters
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getProgramName() {
        return programName;
    }

    public void setProgramName(String programName) {
        this.programName = programName;
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public Long getGroupID() {
        return groupID;
    }

    public void setGroupID(Long groupID) {
        this.groupID = groupID;
    }

    @Override
    public String toString() {
        return "Record{" +
                "status='" + status + '\'' +
                ", programName='" + programName + '\'' +
                ", carrier='" + carrier + '\'' +
                ", groupID=" + groupID +
                '}';
    }
}
