package org.example;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

class RunnableHandleMessage implements Runnable {
    private Thread t;
    private String threadName;
    private JSONObject jsonObject;

    RunnableHandleMessage(String name, JSONObject object) {
        threadName = name;
        jsonObject = object;
//            System.out.println("Creating " +  threadName );
    }

    public void run() {

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
            String mode = null;
            if(jsonObject.has("Mode")){
                mode = jsonObject.getString("Mode");
            }else {
            }

            //写入数据库
            if(distance>=0 && humidity>=0 && temperature>=0 && id>0 && mode!=null){
                Database db = new Database();

                //计算是否已满或者已被清空
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

                        // 如果已经清空且之前未满，需要更新预估数据，更新清空时间
                        if( judgeIfEmptied(depth, previousDistance, distance) && !judgeIfFull(depth, previousDistance)){
                            System.out.println("如果已经清空且之前未满，需要更新预估数据，更新清空时间");
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
                        // 如果已经满了且之前未满，需要更新预估数据
                        else if( judgeIfFull(depth, distance) && !judgeIfFull(depth, previousDistance)){
                            System.out.println("如果已经满了且之前未满，需要更新预估数据");
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
                        // 如果已经清空且之前已满，无需更新预估数据，更新清空时间
                        else if(judgeIfEmptied(depth, previousDistance, distance) && judgeIfFull(depth, previousDistance)){
                            System.out.println("如果已经清空且之前已满，无需更新预估数据，更新清空时间");
                            Object[] obj1 = {distance, humidity, temperature, formatterDateTime.format(date), mode, id};
                            int result1 = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=?, LastEmptyTime=?, Mode=? WHERE Id=?", obj1);
                        }
                        // 其他情况：现在已满且之前就已满，和未满且未被清空
                        else {
                            //更新TrashCan表对应垃圾桶数据
                            System.out.println("其他情况：现在已满且之前就已满，和未满且未被清空");
                            Object[] obj1 = {distance, humidity, temperature, mode, id};
                            int result1 = db.update("UPDATE TrashCan SET Distance=?, Humidity=?, Temperature=?, Mode=? WHERE Id=?", obj1);
                        }
                    }
                }
                //插入record表对应垃圾桶数据
                Object[] obj2 = {distance, humidity, temperature, formatterDateTime.format(date),mode, id};
                int result2 = db.update("insert into record(Distance,Humidity,Temperature,DateTime,Mode, TrashCanId) values(?,?,?,?,?,?)", obj2);
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
        //接收到安卓客户端登录请求时
        //TODO:后续可以考虑在子线程里完成以下操作
        if(dataType!=null && dataType.equals("LoginRequest")){
            //客户端id
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
        //接收到安卓客户端注册请求时
        //TODO:后续可以考虑在子线程里完成以下操作
        if(dataType!=null && dataType.equals("RegisterRequest")){
            //客户端id
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
        //接收到安卓客户端修改用户名请求时
        //TODO:后续可以考虑在子线程里完成以下操作
        if(dataType!=null && dataType.equals("EditUserNameRequest")){
            //客户端id
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
        //接收到安卓客户端修改密码请求时
        //TODO:后续可以考虑在子线程里完成以下操作
        if(dataType!=null && dataType.equals("EditPasswordRequest")){
            //客户端id
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
