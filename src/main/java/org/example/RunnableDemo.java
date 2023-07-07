package org.example;

class RunnableDemo implements Runnable {
    private Thread t;
    private String threadName;

    RunnableDemo( String name) {
        threadName = name;
//            System.out.println("Creating " +  threadName );
    }

    public void run() {
//            System.out.println("Running " +  threadName );
        try {
            // 定时发送消息测试
            int count = 0;
            while(true){
                MyMqttClient myMQTTClient = new MyMqttClient();
                myMQTTClient.publishMessage("testtopic/1","服务端定时发送数据"+count++,0);
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
}
