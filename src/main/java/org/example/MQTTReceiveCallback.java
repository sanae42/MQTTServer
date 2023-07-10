package org.example;

import net.sf.json.JSONObject;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;


public class MQTTReceiveCallback implements MqttCallback {

    @Override
    public void connectionLost(Throwable cause) {
        // 连接丢失后，一般在这里面进行重连
        System.out.println("连接断开，可以重连");
    }


    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
//        System.out.println("deliveryComplete---------" + token.isComplete());
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        // subscribe后得到的消息会执行到这里面
        System.out.print("接收消息主题 : " + topic);
        System.out.print("   接收消息Qos : " + message.getQos());
        System.out.println("   接收消息内容 : " + new String(message.getPayload()));
        //将json字符串翻译成JSON对象(JSONObject)
        JSONObject jsonObject = JSONObject.fromObject(new String(message.getPayload()));
        int id = jsonObject.getInt("Id");
        int distance;
        if(jsonObject.getString("Distance") != "null"){
            distance = jsonObject.getInt("Distance");
        }else {
            distance = -1;
        }
        int humidity;
        if(jsonObject.getString("Humidity") != "null"){
            humidity = jsonObject.getInt("Humidity");
        }else {
            humidity = -1;
        }
        int temperature;
        if(jsonObject.getString("Temperature") != "null"){
            temperature = jsonObject.getInt("Temperature");
        }else {
            temperature = -1;
        }
        // 如果读取类型错位或无相应内容，会重新连接，可用catch捕获错误“JSONObject["Humidity"] is not a number.”
//        try {
//            int humidity = jsonObject.getInt("Humidity");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("测试打印："+distance+"  "+id);

        Database db = new Database();
        Object[] obj = {distance, humidity, temperature, id};
        int i = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=? WHERE Id=?", obj);

    }
}

