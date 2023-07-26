package org.example;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
                List<TrashCanBean> trashCanBeanList = new ArrayList<>();
                ResultSet set = db.select("select * from TrashCan", objs);
                try {
                    while (set.next()) {
                        //将一条记录的数据存入jsonObject
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

                        jsonObject.put("LocationDescription",set.getString("LocationDescription"));
                        jsonObject.put("Mode",set.getString("Mode"));
                        //将jsonObject插入jsonArray
                        jsonArray.add(jsonObject);

                        //将一条记录的数据存入bean
                        TrashCanBean bean = new TrashCanBean();
                        bean.setId(set.getInt("Id"));
                        bean.setDistance(set.getInt("Distance"));
                        bean.setHumidity(set.getInt("Humidity"));
                        bean.setTemperature(set.getInt("Temperature"));
                        bean.setLatitude(set.getDouble("Latitude"));
                        bean.setLongitude(set.getDouble("Longitude"));

                        bean.setDepth(set.getInt("Depth"));
                        bean.setLastEmptyTime((Date)set.getTimestamp("lastEmptyTime"));
                        bean.setEstimatedTime(set.getInt("EstimatedTime"));
                        bean.setVariance(set.getInt("Variance"));

                        bean.setLocationDescription(set.getString("LocationDescription"));
                        bean.setMode(set.getString("Mode"));
                        //将bean插入trashCanBeanList
                        trashCanBeanList.add(bean);
                    }

                    allTrashCanData.put("sender","myMqttClient");
                    allTrashCanData.put("dataType","allTrashCanData");
                    allTrashCanData.put("payload",jsonArray);

                    MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                    myMQTTClient.publishMessage("MQTTServerPub",allTrashCanData.toString(),0);
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                // 检测垃圾桶是否已满，如果是，则发送附件垃圾桶位置信息到该垃圾桶
                try{
                    for(TrashCanBean bean0 : trashCanBeanList){
                        double percentage0 = (double)(bean0.getDepth() - bean0.getDistance()) / (double)bean0.getDepth();
                        if(percentage0 > 0.9){
                            TrashCanBean nearestTrashCan = trashCanBeanList.get(0);
                            for(TrashCanBean bean1 : trashCanBeanList){
                                double percentage1 = (double)(bean1.getDepth() - bean1.getDistance()) / (double)bean1.getDepth();
                                if (percentage1 <= 0.9 && getGap(bean1,nearestTrashCan) < getGap(bean0,nearestTrashCan)){
                                    nearestTrashCan = bean1;
                                }
                            }
//                            JSONObject nearestTrashCanData = new JSONObject();
                            //发给arduino的数据不宜太长，由于Arduino库冲突导致解析JSON失败，故发送字符串，Arduino通过消息长度判断消息类型
//                            nearestTrashCanData.put("sender","myMqttClient");
//                            nearestTrashCanData.put("dataType","nearestTrashCanData");
//                            nearestTrashCanData.put("payload",nearestTrashCan.getLocationDescription());

                            MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                            myMQTTClient.publishMessage("TrashCanSub/"+bean0.getId(),nearestTrashCan.getLocationDescription(),0);
                        }
                    }

                }catch (Exception e) {
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

    private double getGap(TrashCanBean bean1, TrashCanBean bean2){
        double d1 = bean1.getLatitude() - bean2.getLatitude();
        double d2 = bean1.getLongitude() - bean2.getLongitude();
        return Math.pow(Math.pow(d1,2)+Math.pow(d2,2),0.5);
    }
}
