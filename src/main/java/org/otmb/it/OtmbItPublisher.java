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

    @Value("#{${zmq.payload}}")
    private Map<String,Object> payload;

    public OtmbItPublisher(final OtmbItDataMapper otmbItDataMapper, final ZeroMqMessageHandler zeroMqMessageHandler) {
        this.otmbItDataMapper = otmbItDataMapper;
        this.zeroMqMessageHandler = zeroMqMessageHandler;
    }

    @Scheduled(fixedDelay = 30000, initialDelay = 10000)
    public void scheduleMessagePublishTask() {
        try {
            logger.info("=================================================================================");
            logger.info("Publishing message {} to {}", otmbItDataMapper.toMapJson(payload), pubTopic);
            logger.info("=================================================================================");
            Message<?> testMessage = MessageBuilder.withPayload(otmbItDataMapper.toMapJson(payload)).setHeader("topic", pubTopic).build();
            zeroMqMessageHandler.handleMessage(testMessage).subscribe();
        } catch (JsonProcessingException e) {
            logger.error("Failed to publish message", e);
        }
    }
}
