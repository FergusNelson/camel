/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.rabbitmq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ServiceStatus;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.spi.Synchronization;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;

public class RabbitMQConsumer extends DefaultConsumer {
    
    ExecutorService executor;
    Connection conn;
    List<Channel> channels = new ArrayList<Channel>();

    private final RabbitMQEndpoint endpoint;

    public RabbitMQConsumer(final RabbitMQEndpoint endpoint, final Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        log.info("Starting RabbitMQ consumer");

        executor = endpoint.createExecutor();
        log.debug("Using executor {}", executor);

        conn = endpoint.connect(executor);
        log.debug("Using conn {}", conn);
        
        for (int i = 0; i < this.endpoint.getConcurrentConsumers(); i++) {
            Channel channel = conn.createChannel();
            log.debug("Using channel {}", channel);
    
            channel.exchangeDeclare(endpoint.getExchangeName(),
                    "direct",
                    endpoint.isDurable(),
                    endpoint.isAutoDelete(),
                    new HashMap<String, Object>());
            
            // need to make sure the queueDeclare is same with the exchange declare
            channel.queueDeclare(endpoint.getQueue(), endpoint.isDurable(), false, endpoint.isAutoDelete(), null);
            channel.queueBind(endpoint.getQueue(), endpoint.getExchangeName(),
                    endpoint.getRoutingKey() == null ? "" : endpoint.getRoutingKey());
            channel.basicQos(this.endpoint.getPrefetchCount());
            channel.basicConsume(endpoint.getQueue(), endpoint.isAutoAck(), new RabbitConsumer(this, channel));
            this.channels.add(channel);
        }
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        for (Channel channel : this.channels) {
          if (channel.isOpen()) {
            channel.close();
          }
        }
        log.info("Stopping RabbitMQ consumer");
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception ignored) { 
                // ignored
            }
        }

        this.channels = null;
        this.conn = null;
        if (executor != null) {
            if (getEndpoint() != null && getEndpoint().getCamelContext() != null) {
                getEndpoint().getCamelContext().getExecutorServiceManager().shutdownNow(executor);
            } else {
                executor.shutdownNow();
            }
        }
        executor = null;
    }

    class RabbitConsumer extends com.rabbitmq.client.DefaultConsumer {

        private final RabbitMQConsumer consumer;
        private final Channel channel;

        /**
         * Constructs a new instance and records its association to the passed-in channel.
         *
         * @param channel the channel to which this consumer is attached
         */
        public RabbitConsumer(final RabbitMQConsumer consumer, final Channel channel) {
            super(channel);
            this.consumer = consumer;
            this.channel = channel;
        }

        @Override
        public void handleDelivery(final String consumerTag,
                                   final Envelope envelope,
                                   final AMQP.BasicProperties properties,
                                   final byte[] body) throws IOException {

            Exchange exchange = consumer.endpoint.createRabbitExchange(envelope, body);
            mergeAmqpProperties(exchange, properties);
            log.trace("Created exchange [exchange={}]", new Object[]{exchange});

            try {
                if ((!ServiceStatus.Stopped.equals(exchange.getContext().getStatus())) && (!ServiceStatus.Stopping.equals(exchange.getContext().getStatus()))) {
                    if (!this.consumer.endpoint.isAutoAck()) {
                        final long deliveryTag = envelope.getDeliveryTag();
                        exchange.addOnCompletion(new Synchronization() {
                            @Override
                            public void onFailure(final Exchange arg0) {
                                RabbitMQConsumer.this.log.trace("Not acknowledging receipt. Exchange failed [delivery_tag={}]", Long.valueOf(deliveryTag));
                            }
                        
                            @Override
                            public void onComplete(final Exchange arg0) {
                                try {
                                    log.trace("Acknowledging receipt [delivery_tag={}]", Long.valueOf(deliveryTag));
                                    channel.basicAck(deliveryTag, false);
                                } catch (IOException e) {
                                    RabbitMQConsumer.this.log.error("Exception occurred when Acknowledging [delivery_tag={}", Long.valueOf(deliveryTag), e);
                                }
                            }
                        });
                    }
                    this.consumer.getProcessor().process(exchange);
                }
            }
            catch (Exception e) {
                RabbitMQConsumer.this.getExceptionHandler().handleException("Error processing exchange", exchange, e);
            }
        }
        
        private void mergeAmqpProperties(final Exchange exchange, final AMQP.BasicProperties properties)
        {
          if (properties.getType() != null) {
            exchange.getIn().setHeader("rabbitmq.TYPE", properties.getType());
          }
          if (properties.getAppId() != null) {
            exchange.getIn().setHeader("rabbitmq.APP_ID", properties.getAppId());
          }
          if (properties.getClusterId() != null) {
            exchange.getIn().setHeader("rabbitmq.CLUSTERID", properties.getClusterId());
          }
          if (properties.getContentEncoding() != null) {
            exchange.getIn().setHeader("rabbitmq.CONTENT_ENCODING", properties.getContentEncoding());
          }
          if (properties.getContentType() != null) {
            exchange.getIn().setHeader("rabbitmq.CONTENT_TYPE", properties.getContentType());
          }
          if (properties.getCorrelationId() != null) {
            exchange.getIn().setHeader("rabbitmq.CORRELATIONID", properties.getCorrelationId());
          }
          if (properties.getExpiration() != null) {
            exchange.getIn().setHeader("rabbitmq.EXPIRATION", properties.getExpiration());
          }
          if (properties.getMessageId() != null) {
            exchange.getIn().setHeader("rabbitmq.MESSAGE_ID", properties.getMessageId());
          }
          if (properties.getPriority() != null) {
            exchange.getIn().setHeader("rabbitmq.PRIORITY", properties.getPriority());
          }
          if (properties.getReplyTo() != null) {
            exchange.getIn().setHeader("rabbitmq.REPLY_TO", properties.getReplyTo());
          }
          if (properties.getTimestamp() != null) {
            exchange.getIn().setHeader("rabbitmq.TIMESTAMP", properties.getTimestamp());
          }
          if (properties.getUserId() != null) {
            exchange.getIn().setHeader("rabbitmq.USERID", properties.getUserId());
          }
        }
    }
}

