package org.example.pojo;

import java.util.HashMap;

public class Data_pojo {

    private String eventDate;

    private String eventType;

    private HashMap<String, String> eventBody;

    public Data_pojo(String eventDate, String eventType, HashMap<String, String> eventBody) {
        this.eventDate = eventDate;
        this.eventType = eventType;
        this.eventBody = eventBody;
    }

    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public HashMap<String, String> getEventBody() {
        return eventBody;
    }

    public void setEventBody(HashMap<String, String> eventBody) {
        this.eventBody = eventBody;
    }
}
