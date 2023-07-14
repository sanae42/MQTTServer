package org.example;

import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;

// 继承自MqttCallbackExtended而非MqttCallback，可以重写connectComplete方法。MqttCallbackExtended继承自connectComplete
public class MQTTReceiveCallback implements MqttCallbackExtended {
    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        //断开连接必须重新订阅才能收到之前订阅的session连接的消息
        if(reconnect){
//            Log.e("MqttCallbackBus_重连订阅主题", "MQTT_connectComplete:");
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        // 连接丢失后，进行重连
        System.out.println("连接断开，可以重连");
//        cause.printStackTrace();
        //TODO:测试自动重连 此处的自动重连可以正常工作，和前面的mqttConnectOptions.setAutomaticReconnect(true);功能重复，但是暂未发现冲突
        MyMqttClient myMQTTClient = MyMqttClient.getInstance();
        myMQTTClient.reConnect();
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
//        { "Id": 1, "Distance":42, "Humidity":50, "Temperature":25}
        // 如果读取类型错位或无相应内容，会重新连接，可用catch捕获错误“JSONObject["Humidity"] is not a number.”
        JSONObject jsonObject;
        boolean JSON_success = true;
        try{
            jsonObject = JSONObject.fromObject(new String(message.getPayload()));
        }catch (Exception e){
            JSON_success = false;
            System.out.println("JSON转换出错 : " + e.getMessage());
        }

        // 当JSON转换成功时，继续读取数据并写入数据库
        if(JSON_success){
            jsonObject = JSONObject.fromObject(new String(message.getPayload()));
            int id = 0;
            if(jsonObject.has("Id")){
                id = jsonObject.getInt("Id");
            }else {
                id = 0;
            }
            int distance = -1;
            if(jsonObject.has("Distance")){
                distance = jsonObject.getInt("Distance");
            }else {
                distance = -1;
            }
            int humidity = -1;
            if(jsonObject.has("Humidity")){
                humidity = jsonObject.getInt("Humidity");
            }else {
                humidity = -1;
            }
            int temperature = -1;
            if(jsonObject.has("Temperature")){
                temperature = jsonObject.getInt("Temperature");
            }else {
                temperature = -1;
            }

            //写入数据库
            if(distance>=0 && humidity>=0 && temperature>=0 && id>0){
                Database db = new Database();
                Object[] obj = {distance, humidity, temperature, id};
                int i = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=? WHERE Id=?", obj);
            }
        }
    }
}




