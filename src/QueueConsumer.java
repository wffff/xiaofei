import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.bson.Document;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 队列消息-接收（消费）者
 * <p>
 * ClassName: QueueConsumer
 * </p>
 * <p>
 * Copyright: (c)2017 JASTAR·WANG,All rights reserved.
 * </p>
 *
 * @author Jastar·Wang
 * @date 2017-11-15
 */
public class QueueConsumer {

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    // 注意这里是消息接收（消费）者
    private MessageConsumer messageConsumer;
    public static final String QUEUE_NAME = "device1";

    public static void main(String[] args) {
        QueueConsumer consumer = new QueueConsumer();
        consumer.doReceive();

//        Map<String,Object> m= null;
//        try {
//            m = CodeEraser("01031075497B446666E63F516C64E974D1164121C2");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println(m.get("speed"));
    }

    public static Double Hex2Float(String str) {
        str = str.replace(" ", "");
        StringBuffer sb = new StringBuffer();
        int le = str.toCharArray().length;
        for (int i = 1; i <= str.toCharArray().length / 2; i++) {
            sb.append(str, le - i * 2, le - i * 2 + 2);
        }
        float f = Float.intBitsToFloat(Integer.parseInt(sb.toString(), 16));
        return Double.parseDouble(String.valueOf(f));
    }

    public static Double Hex2Double(String str) {
        str = str.replace(" ", "");
        StringBuffer sb = new StringBuffer();
        int le = str.toCharArray().length;
        for (int i = 1; i <= str.toCharArray().length / 2; i++) {
            sb.append(str, le - i * 2, le - i * 2 + 2);
        }
        double d = Double.longBitsToDouble(Long.parseLong(sb.toString(), 16));
        return d;
    }

    public static Map<String, Object> CodeEraser(String code) throws Exception {
        Map<String, Object> m = new HashMap<>();
        code = code.replace(" ", "");
        if (code.toCharArray().length != 42) {
            throw new Exception("数据格式不正确");
        }
        m.put("flow", Hex2Float(code.substring(6, 14)));
        m.put("speed", Hex2Float(code.substring(14, 22)));
        m.put("sum", Hex2Double(code.substring(22, 38)));
        System.out.println(m.toString());
        return m;
    }

    public void doReceive() {
        try {
            connectionFactory = new ActiveMQConnectionFactory();
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(Boolean.FALSE, Session.CLIENT_ACKNOWLEDGE);
            destination = session.createQueue(QUEUE_NAME);

            /**
             * 注意：这里要创建一个消息消费，并指定目的地（即消息源队列）
             */
            messageConsumer = session.createConsumer(destination);

            // 方式一：监听接收
            receiveByListener();

            // 方式二：阻塞接收
            // receiveByManual();

            /**
             * 注意：这里不能再关闭对象了
             */
            // messageConsumer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            /**
             * 注意：这里不能再关闭Connection了
             */
            // connection.close();
        }

    }

    /**
     * 通过注册监听器的方式接收消息，属于被动监听
     */
    private void receiveByListener() {
        try {
            messageConsumer.setMessageListener(message -> {
                if (message instanceof TextMessage) {
                    try {
                        TextMessage msg = (TextMessage) message;
                        String str = msg.getText();
                        MongoClient mongoClient = new MongoClient(MongoConstants.HOST, MongoConstants.PORT);
                        MongoDatabase mongoDatabase = mongoClient.getDatabase(MongoConstants.DATABASENAME);
                        MongoCollection<Document> collection = mongoDatabase.getCollection(MongoConstants.DEVICE1);
                        Document document = new Document();
                        String name = str.substring(0, str.lastIndexOf("#"));
                        String code = str.substring(str.lastIndexOf("#") + 1);
                        document.append("name", name);
                        Map<String, Object> m = CodeEraser(code);
                        document.append("flow",m.get("flow"));
                        document.append("speed",m.get("speed"));
                        document.append("sum",m.get("sum"));
                        collection.insertOne(document);
                        System.out.println(str);
                        // 可以通过此方法反馈消息已收到
                        mongoClient.close();
                        msg.acknowledge();
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }

            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过手动去接收消息的方式，属于主动获取
     */
    private void receiveByManual() {
        while (true) {
            try {
                /**
                 * 通过receive()方法阻塞接收消息，参数为超时时间（单位：毫秒）
                 */
                TextMessage message = (TextMessage) messageConsumer
                        .receive(60000);
                if (message != null) {
                    System.out.println("Received:“" + message.getText() + "”");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
