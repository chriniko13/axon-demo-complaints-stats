package com.example.democomplaintsstats;

import com.example.event.ComplaintFiledEvent;
import com.rabbitmq.client.Channel;
import org.axonframework.amqp.eventhandling.DefaultAMQPMessageConverter;
import org.axonframework.amqp.eventhandling.spring.SpringAMQPMessageSource;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.serialization.Serializer;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
public class DemoComplaintsStatsApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoComplaintsStatsApplication.class, args);
    }


    @ProcessingGroup(value = "statistics")
    @RestController
    public static class ComplaintStatisticsAPI {

        private ConcurrentMap<String, AtomicLong> statistics = new ConcurrentHashMap<>();

        @EventHandler
        public void on(ComplaintFiledEvent complaintFiledEvent) {
            statistics.computeIfAbsent(complaintFiledEvent.getCompany(), k -> new AtomicLong()).incrementAndGet();
        }

        @GetMapping
        public Map<String, AtomicLong> getStatistics() {
            return statistics;
        }
    }


    @Bean
    public SpringAMQPMessageSource complaintEventsQueue(Serializer serializer) {

        return new SpringAMQPMessageSource(new DefaultAMQPMessageConverter(serializer)) {

            @RabbitListener(queues = {"ComplaintEvents"})
            @Override
            public void onMessage(Message message, Channel channel) throws Exception {
                super.onMessage(message, channel);
            }

        };

    }
}
