package org.helloworld;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Article {
    private Long id;
    private String name;
    private String content;
    private Date publishTime;
    private double price;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Date getPublishTime() {
        return publishTime;
    }

    public void setPublishTime(Date publishTime) {
        this.publishTime = publishTime;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public Map<String,Object> toMap(){
        Map<String,Object> map = new HashMap<>();
        map.put("id", id);
        map.put("name", name);
        map.put("content", content);
        map.put("publishTime", publishTime);
        map.put("price", price);
        return map;
    }
}
