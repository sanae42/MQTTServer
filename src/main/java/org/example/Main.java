package org.example;

public class Main {
    private static MyMqttClient myMQTTClient;
    public static void main(String[] args) {
        myMQTTClient = MyMqttClient.getInstance();
        //initialize connection
        myMQTTClient.start();
        //subscribe topics
        myMQTTClient.subTopic("TrashCanPub");
        myMQTTClient.subTopic("MQTTServerSub");
        myMQTTClient.publishMessage("testtopic/1","java client connection test",0);

        //start a thread to sned messages regularly
        RunnableScheduled R1 = new RunnableScheduled( "Thread-1");
        R1.start();

        //test database
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
