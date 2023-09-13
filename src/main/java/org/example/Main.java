package org.example;
/**
 * @Title：Main.java
 * @Description: Main method, initialize myMQTTClient and start a thread to regularly broadcast messages
 * @author P Geng
 */
public class Main {
    private static MyMqttClient myMQTTClient;
    public static void main(String[] args) {
        myMQTTClient = MyMqttClient.getInstance();
        //initialize connection
        myMQTTClient.start();
        //subscribe topics
        myMQTTClient.subTopic("TrashCanPub");
        myMQTTClient.subTopic("MQTTServerSub");
//        myMQTTClient.publishMessage("testtopic/1","java client connection test",0);

        //start a thread to sned messages regularly
        RunnableScheduled R1 = new RunnableScheduled( "Thread-1");
        R1.start();
    }
}
