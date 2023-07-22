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
//  {"dataType":"trashCanDataCollect","Id":1,"Distance":12,"Humidity":57,"Temperature":24.1}
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

            //判断数据类型
            String dataType = null;
            if(jsonObject.has("dataType")){
                dataType = jsonObject.getString("dataType");
            }

            //接收到垃圾桶传感器采集数据时
            //不能用null.equal，必须先判断string是否为null，否则进程会结束
            //TODO:后续可以考虑在子线程里完成以下操作
            if(dataType!=null && dataType.equals("trashCanDataCollect")){
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

                    //计算是否已满或者已被清空
                    Date date = new Date(System.currentTimeMillis());
                    SimpleDateFormat formatterDateTime= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    Object[] obj0 = {id};
                    ResultSet set = db.select("select * from TrashCan where id = ?", obj0);

                    if(set!=null){
                        while (set.next()){
                            int depth = set.getInt("Depth");
                            int previousDistance = set.getInt("Distance");
                            Date lastEmptyTime = set.getTimestamp("LastEmptyTime");
                            int estimatedTime = set.getInt("EstimatedTime");

                            // 如果已经清空且之前未满，需要更新预估数据，更新清空时间
                            if( judgeIfEmptied(depth, previousDistance, distance) && !judgeIfFull(depth, previousDistance)){
                                System.out.println("如果已经清空且之前未满，需要更新预估数据，更新清空时间");
                                int newEstimatedTime = getNewEstimatedTime(lastEmptyTime, date, estimatedTime);
                                int newVariance = getnewVariance(lastEmptyTime, date, id, (depth-distance)/depth, db);
                                Object[] obj1 = {distance, humidity, temperature, formatterDateTime.format(date), newEstimatedTime, newVariance, id};
                                int result1 = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=?, LastEmptyTime=?, EstimatedTime=?, Variance=? WHERE Id=?", obj1);
                            }
                            // 如果已经满了且之前未满，需要更新预估数据
                            else if( judgeIfFull(depth, distance) && !judgeIfFull(depth, previousDistance)){
                                System.out.println("如果已经满了且之前未满，需要更新预估数据");
                                int newEstimatedTime = getNewEstimatedTime(lastEmptyTime, date, estimatedTime);
                                int newVariance = getnewVariance(lastEmptyTime, date, id, (depth-distance)/depth, db);
                                Object[] obj1 = {distance, humidity, temperature, newEstimatedTime, newVariance, id};
                                int result1 = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=?, EstimatedTime=?, Variance=? WHERE Id=?", obj1);
                            }
                            // 如果已经清空且之前已满，无需更新预估数据，更新清空时间
                            else if(judgeIfEmptied(depth, previousDistance, distance) && judgeIfFull(depth, previousDistance)){
                                System.out.println("如果已经清空且之前已满，无需更新预估数据，更新清空时间");
                                Object[] obj1 = {distance, humidity, temperature, formatterDateTime.format(date), id};
                                int result1 = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=?, LastEmptyTime=? WHERE Id=?", obj1);
                            }
                            // 其他情况：现在已满且之前就已满，和未满且未被清空
                            else {
                                //更新TrashCan表对应垃圾桶数据
                                System.out.println("其他情况：现在已满且之前就已满，和未满且未被清空");
                                Object[] obj1 = {distance, humidity, temperature, id};
                                int result1 = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=? WHERE Id=?", obj1);
                            }
                        }
                    }
                    //插入record表对应垃圾桶数据
                    Object[] obj2 = {distance, humidity, temperature, formatterDateTime.format(date), id};
                    int result2 = db.update("insert into record(Distance,Humidity,Temperature,DateTime,TrashCanId) values(?,?,?,?,?)", obj2);
                }
            }
            //接收到安卓客户端数据请求时
            // {"dataType":"trashCanDataRequest","Id":"Android\/1","TrashCanId":1}
            //TODO:后续可以考虑在子线程里完成以下操作
            if(dataType!=null && dataType.equals("trashCanDataRequest")){
                String id = null;
                if(jsonObject.has("Id")){
                    id = jsonObject.getString("Id");
                }
                int TrashCanId = -1;
                if(jsonObject.has("TrashCanId")){
                    TrashCanId = jsonObject.getInt("TrashCanId");
                }
                if(id!=null && TrashCanId>=0) {
                    JSONObject thisTrashCanData = new JSONObject();
                    JSONArray jsonArray = new JSONArray();

                    Database db = new Database();
                    Object[] obj0 = {TrashCanId};
                    ResultSet set = db.select("select * from Record where TrashCanId = ?", obj0);
                    if(set!=null){
                        while (set.next()){
                            JSONObject obj = new JSONObject();
                            obj.put("Id", set.getInt("id"));
                            obj.put("TrashCanId", set.getInt("TrashCanId"));
                            obj.put("Distance", set.getInt("Distance"));
                            obj.put("Humidity", set.getInt("Humidity"));
                            obj.put("Temperature", set.getInt("Temperature"));
                            obj.put("DateTime",(Date)set.getTimestamp("DateTime"));

                            jsonArray.add(obj);
                        }
                    }
                    thisTrashCanData.put("sender","myMqttClient");
                    thisTrashCanData.put("dataType","thisTrashCanData");
                    thisTrashCanData.put("TrashCanId",TrashCanId);
                    thisTrashCanData.put("payload",jsonArray);

                    MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                    myMQTTClient.publishMessage(id,thisTrashCanData.toString(),0);
                }
            }

        }
    }

    private boolean judgeIfFull(int depth, int distance){
        float percentage = (float)(depth-distance)/(float) depth;
        //垃圾桶已满
        if(percentage > 0.9){
            return true;
        }
        return false;
    }

    private boolean judgeIfEmptied(int depth, int previousDistance, int distance){
        //判定为已被清空
        if((depth-distance) < 10 && (depth-distance)+20 < (depth-previousDistance)){
            return true;
        }
        return false;
    }

    private int getNewEstimatedTime(Date lastEmptyTime, Date date, int estimatedTime){
        //计算时间差
        long diff = date.getTime() - lastEmptyTime.getTime(); //毫秒单位
        int hourDiff = (int)( diff / (1000 * 60 * 60) ); //小时单位
        //计算预估时间
        double alpha = 0.2;
        int newEstimatedTime = (int)( alpha*hourDiff + (1-alpha)*estimatedTime );

        return newEstimatedTime;
    }

    private int getnewVariance(Date lastEmptyTime, Date date, int id, double perc, Database db) throws SQLException {
        int limitNum = 15; //timeConsume记录的条数
        Object[] obj = {id,limitNum};
        ResultSet timeSet = db.select("select * from Time where TrashCanId = ? ORDER BY id DESC LIMIT ?", obj);
        List<Integer> timeConsumeList = new ArrayList<>();
        if(timeSet!=null){
            while (timeSet.next()){
                timeConsumeList.add(timeSet.getInt("TimeConsume"));
            }
        }
        //计算时间差
        long diff = date.getTime() - lastEmptyTime.getTime(); //毫秒单位
        int hourDiff = (int)( diff / (1000 * 60 * 60) ); //小时单位
        //预估装满所需时间
        hourDiff /= perc;

        //在数据库time表种插入新的耗时记录
        Object[] obj2 = {hourDiff, id};
        int result2 = db.update("insert into time(TimeConsume,TrashCanId) values(?,?)", obj2);

        //计算方差
        timeConsumeList.add(hourDiff);
//        for(int i:timeConsumeList){
//            System.out.println(i);
//        }
        double sum = 0;
        for(int i : timeConsumeList){
            sum += i;
        }
        double ave = sum / timeConsumeList.size();
        double v = 0;
        for(int i : timeConsumeList){
            if(i<=0){
                v+=0;
            }else {
                v += Math.pow(i-ave, 2);
            }
        }
        int newVariance = (int)v / timeConsumeList.size();

        return newVariance;
    }

}




