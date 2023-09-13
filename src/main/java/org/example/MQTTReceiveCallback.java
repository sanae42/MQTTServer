package org.example;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Title：MQTTReceiveCallback.java
 * @Description: When an event related to mqttClient occurs, the program will call the corresponding method in MQTTReceiveCallback.
 * @author P Geng
 */
public class MQTTReceiveCallback implements MqttCallbackExtended {
    //Inheriting from MqttCallbackExtended instead of MqttCallback ,can override the connectComplete method. MqttCallbackExtended inherits from connectComplete

    /**
     * Called when successfully connecting to the broker
     */
    @Override
    public void connectComplete(boolean reconnect, String serverURI) {

    }

    /**
     *  Called when disconnected, then get the MyMqttClient instance and call reConnect()
     */
    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("The connection is disconnected and can be reconnected");
        MyMqttClient myMQTTClient = MyMqttClient.getInstance();
        myMQTTClient.reConnect();
    }

    /**
     * Called when the message is successfully delivered
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
//        System.out.println("deliveryComplete---------" + token.isComplete());
    }

    /**
     * Called when a new message is received
     */
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.print("Received a message:   ");
        System.out.print("topic : " + topic);
        System.out.print("   qos : " + message.getQos());
        System.out.println("   content : " + new String(message.getPayload()));

        //parse the JSON string of the message and responds accordingly to messages from different senders
        JSONObject jsonObject;
        boolean JSON_success = true;
        try{
            jsonObject = JSONObject.fromObject(new String(message.getPayload()));
        }catch (Exception e){
            JSON_success = false;
            System.out.println("JSON conversion error : " + e.getMessage());
        }

        // When JSON conversion is successful, continue to read data and write it to the database
        if(JSON_success) {
            jsonObject = JSONObject.fromObject(new String(message.getPayload()));
            //Start Thread
            RunnableHandleMessage R = new RunnableHandleMessage( "Thread-1", jsonObject);
            R.start();
        }
    }


}




