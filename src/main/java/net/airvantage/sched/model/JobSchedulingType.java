package net.airvantage.sched.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum JobSchedulingType {
    
    CRON("cron"),
    WAKEUP("wakeup");
    
    private String key;

    JobSchedulingType(String key) {
        this.key = key;
    }

    @JsonCreator
    public static JobSchedulingType newInstance(String key) {
        return JobSchedulingType.valueOf(key.toUpperCase());
    }

    @JsonValue
    public String getKey() {
        return key;
    }
    
}
