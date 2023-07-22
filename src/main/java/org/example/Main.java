package org.example;

public class Main {
    private static MyMqttClient myMQTTClient;
    public static void main(String[] args) {
        myMQTTClient = MyMqttClient.getInstance();
        //初始化连接
        myMQTTClient.start();
        //订阅/World这个主题
        myMQTTClient.subTopic("TrashCanPub");
        myMQTTClient.subTopic("MQTTServerSub");
        myMQTTClient.publishMessage("testtopic/1","java客户端连接测试",0);

        //开启线程定时执行
        RunnableScheduled R1 = new RunnableScheduled( "Thread-1");
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
