package org.otmb.it;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.zeromq.outbound.ZeroMqMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class OtmbItPublisher {
    private static final Logger logger = LoggerFactory.getLogger(OtmbItPublisher.class);
    private final OtmbItDataMapper otmbItDataMapper;

    private final ZeroMqMessageHandler zeroMqMessageHandler;

    @Value("${zmq.channel.pubTopic}")
    private String pubTopic;

    public OtmbItPublisher(final OtmbItDataMapper otmbItDataMapper, final ZeroMqMessageHandler zeroMqMessageHandler) {
        this.otmbItDataMapper = otmbItDataMapper;
        this.zeroMqMessageHandler = zeroMqMessageHandler;
    }

    @Scheduled(fixedDelay = 60000, initialDelay = 10000)
    public void scheduleMessagePublishTask() {
        Map<String, Object> controlMessage = new HashMap<>();
        controlMessage.put("DCTE.RtnDlyTmms.AO10", 15);
        controlMessage.put("DCTE.VLoLim.AO7", 20);

        try {
            logger.info("=================================================================================");
            logger.info("Publishing message {} to {}", otmbItDataMapper.toMapJson(controlMessage), pubTopic);
            logger.info("=================================================================================");
            Message<?> testMessage = MessageBuilder.withPayload(otmbItDataMapper.toMapJson(controlMessage)).setHeader("topic", pubTopic).build();
            zeroMqMessageHandler.handleMessage(testMessage).subscribe();
        } catch (JsonProcessingException e) {
            logger.error("Failed to publish message", e);
        }
    }
}
