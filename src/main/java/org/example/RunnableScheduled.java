package org.example;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

class RunnableScheduled implements Runnable {
    private Thread t;
    private String threadName;

    RunnableScheduled(String name) {
        threadName = name;
//            System.out.println("Creating " +  threadName );
    }

    public void run() {
//            System.out.println("Running " +  threadName );
        try {
            // 定时发送消息测试
//            int count = 0;
            while(true){
                //全部垃圾桶状态数据
                JSONObject allTrashCanData = new JSONObject();
                JSONArray jsonArray = new JSONArray();

                Database db = new Database();
                Object[] objs = {};
                ResultSet set = db.select("select * from TrashCan", objs);
                try {
                    while (set.next()) {
//                        int id = set.getInt("Id");
//                        int distance = set.getInt("Distance");
//                        int humidity = set.getInt("Humidity");
//                        int temperature = set.getInt("Temperature");
//                        System.out.println(id + " " + distance + " " + humidity + " " + temperature);
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("Id", set.getInt("Id"));
                        jsonObject.put("Distance", set.getInt("Distance"));
                        jsonObject.put("Humidity", set.getInt("Humidity"));
                        jsonObject.put("Temperature", set.getInt("Temperature"));
                        jsonObject.put("Latitude", set.getDouble("Latitude"));
                        jsonObject.put("Longitude", set.getDouble("Longitude"));

                        jsonObject.put("Depth",set.getInt("Depth"));
                        jsonObject.put("LastEmptyTime",(Date)set.getTimestamp("lastEmptyTime"));
                        jsonObject.put("EstimatedTime",set.getInt("EstimatedTime"));
                        jsonObject.put("Variance",set.getInt("Variance"));

                        jsonArray.add(jsonObject);
                    }

                    allTrashCanData.put("sender","myMqttClient");
                    allTrashCanData.put("dataType","allTrashCanData");
                    allTrashCanData.put("payload",jsonArray);

                    MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                    myMQTTClient.publishMessage("MQTTServerPub",allTrashCanData.toString(),0);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
//
//
//                MyMqttClient myMQTTClient = MyMqttClient.getInstance();
//                myMQTTClient.publishMessage("testtopic/1","服务端定时发送数据"+count++,0);
                Thread.sleep(5000);
            }
        }catch (InterruptedException e) {
            System.out.println("Thread " +  threadName + " interrupted.");
        }
        System.out.println("Thread " +  threadName + " exiting.");
    }

    public void start () {
//            System.out.println("Starting " +  threadName );
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }
}