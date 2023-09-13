package org.example;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Title：RunnableHandleMessage.java
 * @Description: A thread that processes received MQTT messages and takes corresponding actions
 * @author P Geng
 */
class RunnableHandleMessage implements Runnable {
    private Thread t;
    private String threadName;
    private JSONObject jsonObject;

    RunnableHandleMessage(String name, JSONObject object) {
        threadName = name;
        jsonObject = object;
    }

    public void run() {
        //Determine data type
        String dataType = null;
        if(jsonObject.has("dataType")){
            dataType = jsonObject.getString("dataType");
        }

        //When receiving data from the trash can sensor
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
            String mode = null;
            if(jsonObject.has("Mode")){
                mode = jsonObject.getString("Mode");
            }else {
            }

            //Write to database
            if(distance>=0 && humidity>=0 && temperature>=0 && id>0 && mode!=null){
                Database db = new Database();

                //Calculate whether the trash can is full or empty
                Date date = new Date(System.currentTimeMillis());
                SimpleDateFormat formatterDateTime= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                Object[] obj0 = {id};
                ResultSet set = db.select("select * from TrashCan where id = ?", obj0);

                if(set!=null){
                    while (true){
                        try {
                            if (!set.next()) break;
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        int depth;
                        int previousDistance;
                        Date lastEmptyTime;
                        int estimatedTime;

                        try {
                            depth = set.getInt("Depth");
                            previousDistance = set.getInt("Distance");
                            lastEmptyTime = set.getTimestamp("LastEmptyTime");
                            estimatedTime = set.getInt("EstimatedTime");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        // If it has already been cleared and was not previously full,
                        // it is necessary to update the estimated data and update the clearing time
                        if( judgeIfEmptied(depth, previousDistance, distance) && !judgeIfFull(depth, previousDistance)){
                            int newEstimatedTime = getNewEstimatedTime(lastEmptyTime, date, estimatedTime);
                            int newVariance = 0;
                            try {
                                newVariance = getnewVariance(lastEmptyTime, date, id, (depth-distance)/depth, db);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            Object[] obj1 = {distance, humidity, temperature, formatterDateTime.format(date), newEstimatedTime, newVariance, mode, id};
                            int result1 = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=?, LastEmptyTime=?, EstimatedTime=?, Variance=?, Mode=? WHERE Id=?", obj1);
                        }
                        // If it is already full and not previously full, the estimated data needs to be updated
                        else if( judgeIfFull(depth, distance) && !judgeIfFull(depth, previousDistance)){
                            int newEstimatedTime = getNewEstimatedTime(lastEmptyTime, date, estimatedTime);
                            int newVariance = 0;
                            try {
                                newVariance = getnewVariance(lastEmptyTime, date, id, (depth-distance)/depth, db);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            Object[] obj1 = {distance, humidity, temperature, newEstimatedTime, newVariance, mode, id};
                            int result1 = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=?, EstimatedTime=?, Variance=?, Mode=? WHERE Id=?", obj1);
                        }
                        // If it has already been cleared and was previously full,
                        // there is no need to update the estimated data and update the clearing time
                        else if(judgeIfEmptied(depth, previousDistance, distance) && judgeIfFull(depth, previousDistance)){
                            Object[] obj1 = {distance, humidity, temperature, formatterDateTime.format(date), mode, id};
                            int result1 = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=?, LastEmptyTime=?, Mode=? WHERE Id=?", obj1);
                        }
                        // Other situations: now full and previously full, and not full and not cleared
                        else {
                            //Update the corresponding trash can data in the TrashCan table
                            Object[] obj1 = {distance, humidity, temperature, mode, id};
                            int result1 = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=?, Mode=? WHERE Id=?", obj1);
                        }
                    }
                }
                //Insert the corresponding trash can data in the record table
                Object[] obj2 = {distance, humidity, temperature, formatterDateTime.format(date),mode, id};
                int result2 = db.update("insert into record(Distance,Humidity,Temperature,DateTime,Mode, TrashCanId) values(?,?,?,?,?,?)", obj2);
            }
        }
        //When receiving trash can data requests from Android clients
        // {"dataType":"trashCanDataRequest","Id":"Android\/1","TrashCanId":1}
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
                    try {
                        while (set.next()){
                            JSONObject obj = new JSONObject();
                            obj.put("Id", set.getInt("id"));
                            obj.put("TrashCanId", set.getInt("TrashCanId"));
                            obj.put("Distance", set.getInt("Distance"));
                            obj.put("Humidity", set.getInt("Humidity"));
                            obj.put("Temperature", set.getInt("Temperature"));
                            obj.put("DateTime",(Date)set.getTimestamp("DateTime"));
                            obj.put("Mode",set.getString("Mode"));

                            jsonArray.add(obj);
                        }
                    }catch (Exception e){

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
        //When receiving an Android client login request
        if(dataType!=null && dataType.equals("LoginRequest")){
            //client id
            String id = null;
            if(jsonObject.has("Id")){
                id = jsonObject.getString("Id");
            }
            //UserName
            String UserName = null;
            if(jsonObject.has("UserName")){
                UserName = jsonObject.getString("UserName");
            }
            //Password
            String Password = null;
            if(jsonObject.has("Password")){
                Password = jsonObject.getString("Password");
            }
            if(id!=null && UserName!=null && Password!=null){
                Database db = new Database();
                Object[] obj0 = {UserName, Password};
                ResultSet set = db.select("select * from User where UserName = ? And Password = ?", obj0);

                JSONObject loginReplyData = new JSONObject();
                loginReplyData.put("sender","myMqttClient");
                loginReplyData.put("dataType","loginReplyData");
                try {
                    if(set.next()){
                        loginReplyData.put("result","succeeded");
                        loginReplyData.put("UserName",UserName);
                        MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                        myMQTTClient.publishMessage(id,loginReplyData.toString(),0);
                    }else {
                        loginReplyData.put("result","failed");
                        MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                        myMQTTClient.publishMessage(id,loginReplyData.toString(),0);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        //When receiving an Android client registration request
        if(dataType!=null && dataType.equals("RegisterRequest")){
            //client id
            String id = null;
            if(jsonObject.has("Id")){
                id = jsonObject.getString("Id");
            }
            //UserName
            String UserName = null;
            if(jsonObject.has("UserName")){
                UserName = jsonObject.getString("UserName");
            }
            //Password
            String Password = null;
            if(jsonObject.has("Password")){
                Password = jsonObject.getString("Password");
            }
            if(id!=null && UserName!=null && Password!=null){
                Database db = new Database();
                Object[] obj0 = {UserName};
                ResultSet set = db.select("select * from User where UserName = ?", obj0);

                JSONObject registerReplyData = new JSONObject();
                registerReplyData.put("sender","myMqttClient");
                try {
                    if(!set.next()){
                        //set为空时
                        Object[] obj2 = {UserName, Password};
                        int result2 = db.update("insert into user(UserName, Password) values(?,?)", obj2);
                        registerReplyData.put("UserName",UserName);
                        registerReplyData.put("dataType","registerReplyData");
                        registerReplyData.put("result","succeeded");
                        MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                        myMQTTClient.publishMessage(id,registerReplyData.toString(),0);
                    }else {
                        registerReplyData.put("dataType","registerReplyData");
                        registerReplyData.put("result","failed");
                        MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                        myMQTTClient.publishMessage(id,registerReplyData.toString(),0);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        //When receiving a user name modification request from an Android client
        if(dataType!=null && dataType.equals("EditUserNameRequest")){
            //client id
            String id = null;
            if(jsonObject.has("Id")){
                id = jsonObject.getString("Id");
            }
            //UserName
            String UserName = null;
            if(jsonObject.has("UserName")){
                UserName = jsonObject.getString("UserName");
            }
            //newUserName
            String newUserName = null;
            if(jsonObject.has("newUserName")){
                newUserName = jsonObject.getString("newUserName");
            }
            if(id!=null && UserName!=null && newUserName!=null){
                Database db = new Database();
                Object[] obj0 = {UserName};
                ResultSet set = db.select("select * from User where UserName = ?", obj0);

                JSONObject registerReplyData = new JSONObject();
                registerReplyData.put("sender","myMqttClient");
                registerReplyData.put("dataType","editUserNameReplyData");
                try {
                    if(set.next()){
                        Object[] obj2 = {newUserName, UserName};
                        int result2 = db.update("update user set UserName = ? where UserName = ?", obj2);
                        registerReplyData.put("UserName",newUserName);
                        registerReplyData.put("result","succeeded");
                        MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                        myMQTTClient.publishMessage(id,registerReplyData.toString(),0);
                    }else {
                        registerReplyData.put("result","failed");
                        MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                        myMQTTClient.publishMessage(id,registerReplyData.toString(),0);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        //When receiving a password modification request from an Android client
        if(dataType!=null && dataType.equals("EditPasswordRequest")){
            //client id
            String id = null;
            if(jsonObject.has("Id")){
                id = jsonObject.getString("Id");
            }
            //UserName
            String UserName = null;
            if(jsonObject.has("UserName")){
                UserName = jsonObject.getString("UserName");
            }
            //newUserName
            String Password = null;
            if(jsonObject.has("Password")){
                Password = jsonObject.getString("Password");
            }
            if(id!=null && UserName!=null && Password!=null){
                Database db = new Database();
                Object[] obj0 = {UserName};
                ResultSet set = db.select("select * from User where UserName = ?", obj0);

                JSONObject registerReplyData = new JSONObject();
                registerReplyData.put("sender","myMqttClient");
                registerReplyData.put("dataType","editPasswordReplyData");
                try {
                    if(set.next()){
                        Object[] obj2 = {Password, UserName};
                        int result2 = db.update("update user set Password = ? where UserName = ?", obj2);
                        registerReplyData.put("result","succeeded");
                        MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                        myMQTTClient.publishMessage(id,registerReplyData.toString(),0);
                    }else {
                        registerReplyData.put("result","failed");
                        MyMqttClient myMQTTClient = MyMqttClient.getInstance();
                        myMQTTClient.publishMessage(id,registerReplyData.toString(),0);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    /**
     * Determine if the trash can is full
     */
    private boolean judgeIfFull(int depth, int distance){
        float percentage = (float)(depth-distance)/(float) depth;
        //The trash can is full
        if(percentage > 0.9){
            return true;
        }
        return false;
    }

    /**
     * Determine if the trash can is emptied
     */
    private boolean judgeIfEmptied(int depth, int previousDistance, int distance){
        //Determined as cleared
        if((depth-distance) < 10 && (depth-distance)+20 < (depth-previousDistance)){
            return true;
        }
        return false;
    }

    /**
     * Calculate the value of EstimatedTime
     */
    private int getNewEstimatedTime(Date lastEmptyTime, Date date, int estimatedTime){
        //Calculate time difference
        long diff = date.getTime() - lastEmptyTime.getTime(); //ms
        int hourDiff = (int)( diff / (1000 * 60 * 60) ); //hour
        //Calculate estimated time
        double alpha = 0.2;
        int newEstimatedTime = (int)( alpha*hourDiff + (1-alpha)*estimatedTime );

        return newEstimatedTime;
    }

    /**
     * Calculate the value of Variance
     */
    private int getnewVariance(Date lastEmptyTime, Date date, int id, double perc, Database db) throws SQLException {
        int limitNum = 15; //Number of recent records considered
        Object[] obj = {id,limitNum};
        ResultSet timeSet = db.select("select * from Time where TrashCanId = ? ORDER BY id DESC LIMIT ?", obj);
        List<Integer> timeConsumeList = new ArrayList<>();
        if(timeSet!=null){
            while (timeSet.next()){
                timeConsumeList.add(timeSet.getInt("TimeConsume"));
            }
        }
        //Calculate time difference
        long diff = date.getTime() - lastEmptyTime.getTime(); //毫秒单位
        int hourDiff = (int)( diff / (1000 * 60 * 60) ); //小时单位
        //Estimated time to fill
        hourDiff /= perc;

        //Insert new time consuming records in the database time table
        Object[] obj2 = {hourDiff, id};
        int result2 = db.update("insert into time(TimeConsume,TrashCanId) values(?,?)", obj2);

        //Calculate variance
        timeConsumeList.add(hourDiff);

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

    public void start () {
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }

}
