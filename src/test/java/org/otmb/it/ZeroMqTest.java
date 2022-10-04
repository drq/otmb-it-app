package org.otmb.it;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.zeromq.channel.ZeroMqChannel;
import org.springframework.integration.zeromq.outbound.ZeroMqMessageHandler;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.zeromq.ZContext;

@SpringBootTest
@ContextConfiguration(classes = TestApp.class)
@ExtendWith(SpringExtension.class)
public class ZeroMqTest {

    private static final Logger logger = LoggerFactory.getLogger(ZeroMqTest.class);

    @Autowired
    private ZeroMqChannel zeroMqPubChannel;

    @BeforeAll
    static void beforeAll() {
    }

    @Test
    void testPubSub() {
        GenericMessage<String> testMessage = new GenericMessage<>("test1");
        zeroMqPubChannel.send(testMessage);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            
        }
    }
}
