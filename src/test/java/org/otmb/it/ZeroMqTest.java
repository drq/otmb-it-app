package org.otmb.it;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.zeromq.channel.ZeroMqChannel;
import org.springframework.integration.zeromq.inbound.ZeroMqMessageProducer;
import org.springframework.integration.zeromq.outbound.ZeroMqMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ContextConfiguration(classes = TestApp.class)
@ExtendWith(SpringExtension.class)
public class ZeroMqTest {

    private static final Logger logger = LoggerFactory.getLogger(ZeroMqTest.class);

    @Value("${zmq.channel.url}")
    private String url;

    @Value("${zmq.channel.topic}")
    private String topic;

    @Value("${zmq.channel.pubTopic}")
    private String pubTopic;

    @Autowired
    private OtmbItDataMapper otmbItDataMapper;

    @Autowired
    private ZContext zContext;

    @Autowired
    private ZeroMqMessageHandler zeroMqMessageHandler;

    @Autowired
    private ZeroMqMessageProducer zeroMqMessageProducer;

    @BeforeAll
    static void beforeAll() {
    }

/*
    @Test
    void testPubSub2() {
        String socketAddress = url;
        ZMQ.Socket socket = zContext.createSocket(SocketType.SUB);
        socket.bind(socketAddress);
        socket.subscribe(topic);

        Message<?> testMessage = MessageBuilder.withPayload("test").setHeader("topic", topic).build();
        zeroMqMessageHandler.handleMessage(testMessage).subscribe();

        //ZMsg msg = ZMsg.recvMsg(socket);
        //logger.info("Message: {}", msg);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {

        }
    }
 */

    @Test
    void testPubSub3() throws JsonProcessingException {
        Map<String, Object> controlMessage = new HashMap<>();
        controlMessage.put("foo", "bar");

        Message<?> testMessage = MessageBuilder.withPayload(otmbItDataMapper.toMapJson(controlMessage)).setHeader("topic", pubTopic).build();
        zeroMqMessageHandler.handleMessage(testMessage).subscribe();

        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {

        }

    }
}
