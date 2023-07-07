package org.example;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Timer;
import java.util.TimerTask;

public class Main {
    private static MyMqttClient myMQTTClient;
    private static String ClientName = "myMqttClient";
    private static String IP = "47.98.247.122";
    public static void main(String[] args) {
        myMQTTClient = new MyMqttClient();
        //初始化连接
        myMQTTClient.start(ClientName);
        //订阅/World这个主题
        myMQTTClient.subTopic("TrashCanPub");

        //开启线程定时执行
        RunnableDemo R1 = new RunnableDemo( "Thread-1");
        R1.start();

        //测试数据库
//        Database db = new Database();
//        Object[] obj = {2, 9, 40, 25};
//        int i = db.update("insert into TrashCan values(?,?,?,?)", obj);
//        System.out.println(i);
//
//        Object[] objs = {};
//        ResultSet set = db.select("select * from TrashCan", objs);
//        try {
//            while (set.next()) {
//                int id = set.getInt("Id");
//                int distance = set.getInt("Distance");
//                int humidity = set.getInt("Humidity");
//                int temperature = set.getInt("Temperature");
//                System.out.println(id + " " + distance + " " + humidity + " " + temperature);
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }

    }

}
