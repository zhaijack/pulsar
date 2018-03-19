/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.resumeChecksum;
import static java.lang.String.format;
import static org.apache.pulsar.common.api.Commands.hasChecksum;
import static org.apache.pulsar.common.api.Commands.readChecksum;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.CryptoException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.ByteBufPair;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.Commands.ChecksumType;
import org.apache.pulsar.common.api.PulsarDecoder;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.util.DateFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Queues;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.ScheduledFuture;

public class ProducerImpl<T> extends ProducerBase<T> implements TimerTask, ConnectionHandler.Connection {

    // Producer id, used to identify a producer within a single connection
    private final long producerId;

    // Variable is used through the atomic updater
    @SuppressWarnings("unused")
    private volatile long msgIdGenerator;

    private final BlockingQueue<OpSendMsg> pendingMessages;
    private final BlockingQueue<OpSendMsg> pendingCallbacks;
    private final Semaphore semaphore;
    private volatile Timeout sendTimeout = null;
    private volatile Timeout batchMessageAndSendTimeout = null;
    private long createProducerTimeout;
    private final int maxNumMessagesInBatch;
    private final BatchMessageContainer batchMessageContainer;

    // Globally unique producer name
    private String producerName;

    private String connectionId;
    private String connectedSince;
    private final int partitionIndex;

    private final ProducerStatsRecorder stats;

    private final CompressionCodec compressor;

    private volatile long lastSequenceIdPublished;
    private MessageCrypto msgCrypto = null;

    private ScheduledFuture<?> keyGeneratorTask = null;

    private final Map<String, String> metadata;

    private final ConnectionHandler connectionHandler;

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<ProducerImpl> msgIdGeneratorUpdater = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "msgIdGenerator");

    public ProducerImpl(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
                        CompletableFuture<Producer<T>> producerCreatedFuture, int partitionIndex, Schema<T> schema) {
        super(client, topic, conf, producerCreatedFuture, schema);
        this.producerId = client.newProducerId();
        this.producerName = conf.getProducerName();
        this.partitionIndex = partitionIndex;
        this.pendingMessages = Queues.newArrayBlockingQueue(conf.getMaxPendingMessages());
        this.pendingCallbacks = Queues.newArrayBlockingQueue(conf.getMaxPendingMessages());
        this.semaphore = new Semaphore(conf.getMaxPendingMessages(), true);
        this.compressor = CompressionCodecProvider
                .getCompressionCodec(convertCompressionType(conf.getCompressionType()));

        if (conf.getInitialSequenceId() != null) {
            long initialSequenceId = conf.getInitialSequenceId();
            this.lastSequenceIdPublished = initialSequenceId;
            this.msgIdGenerator = initialSequenceId + 1;
        } else {
            this.lastSequenceIdPublished = -1;
            this.msgIdGenerator = 0;
        }

        if (conf.isEncryptionEnabled()) {
            String logCtx = "[" + topic + "] [" + producerName + "] [" + producerId + "]";
            this.msgCrypto = new MessageCrypto(logCtx, true);

            // Regenerate data key cipher at fixed interval
            keyGeneratorTask = client.eventLoopGroup().scheduleWithFixedDelay(() -> {
                try {
                    msgCrypto.addPublicKeyCipher(conf.getEncryptionKeys(), conf.getCryptoKeyReader());
                } catch (CryptoException e) {
                    if (!producerCreatedFuture.isDone()) {
                        log.warn("[{}] [{}] [{}] Failed to add public key cipher.", topic, producerName, producerId);
                        producerCreatedFuture.completeExceptionally(e);
                    }
                }
            }, 0L, 4L, TimeUnit.HOURS);

        }

        if (conf.getSendTimeoutMs() > 0) {
            sendTimeout = client.timer().newTimeout(this, conf.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
        }

        this.createProducerTimeout = System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs();
        if (conf.isBatchingEnabled()) {
            this.maxNumMessagesInBatch = conf.getBatchingMaxMessages();
            this.batchMessageContainer = new BatchMessageContainer(maxNumMessagesInBatch,
                    convertCompressionType(conf.getCompressionType()), topic, producerName);
        } else {
            this.maxNumMessagesInBatch = 1;
            this.batchMessageContainer = null;
        }
        if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
            stats = new ProducerStatsRecorderImpl(client, conf, this);
        } else {
            stats = ProducerStatsDisabled.INSTANCE;
        }

        if (conf.getProperties().isEmpty()) {
            metadata = Collections.emptyMap();
        } else {
            metadata = Collections.unmodifiableMap(new HashMap<>(conf.getProperties()));
        }

        this.connectionHandler = new ConnectionHandler(this,
            new Backoff(100, TimeUnit.MILLISECONDS, 60, TimeUnit.SECONDS, Math.max(100, conf.getSendTimeoutMs() - 100), TimeUnit.MILLISECONDS),
            this);
        grabCnx();
    }

    public ConnectionHandler getConnectionHandler() {
        return connectionHandler;
    }

    private boolean isBatchMessagingEnabled() {
        return conf.isBatchingEnabled();
    }

    @Override
    public long getLastSequenceId() {
        return lastSequenceIdPublished;
    }

    @Override
    public CompletableFuture<MessageId> sendAsync(Message<T> message) {
        CompletableFuture<MessageId> future = new CompletableFuture<>();

        sendAsync(message, new SendCallback() {
            SendCallback nextCallback = null;
            long createdAt = System.nanoTime();

            @Override
            public CompletableFuture<MessageId> getFuture() {
                return future;
            }

            @Override
            public SendCallback getNextSendCallback() {
                return nextCallback;
            }

            @Override
            public void sendComplete(Exception e) {
                if (e != null) {
                    stats.incrementSendFailed();
                    future.completeExceptionally(e);
                } else {
                    future.complete(message.getMessageId());
                    stats.incrementNumAcksReceived(System.nanoTime() - createdAt);
                }
                while (nextCallback != null) {
                    SendCallback sendCallback = nextCallback;
                    if (e != null) {
                        stats.incrementSendFailed();
                        sendCallback.getFuture().completeExceptionally(e);
                    } else {
                        sendCallback.getFuture().complete(message.getMessageId());
                        stats.incrementNumAcksReceived(System.nanoTime() - createdAt);
                    }
                    nextCallback = nextCallback.getNextSendCallback();
                    sendCallback = null;
                }
            }

            @Override
            public void addCallback(SendCallback scb) {
                nextCallback = scb;
            }
        });
        return future;
    }

    public void sendAsync(Message<T> message, SendCallback callback) {
        checkArgument(message instanceof MessageImpl);

        if (!isValidProducerState(callback)) {
            return;
        }

        if (!canEnqueueRequest(callback)) {
            return;
        }

        MessageImpl<T> msg = (MessageImpl<T>) message;
        MessageMetadata.Builder msgMetadata = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();

        // If compression is enabled, we are compressing, otherwise it will simply use the same buffer
        int uncompressedSize = payload.readableBytes();
        ByteBuf compressedPayload = payload;
        // batch will be compressed when closed
        if (!isBatchMessagingEnabled()) {
            compressedPayload = compressor.encode(payload);
            payload.release();
        }
        int compressedSize = compressedPayload.readableBytes();

        // validate msg-size (validate uncompressed-payload size for batch as we can't discard later on while building a
        // batch)
        if (compressedSize > PulsarDecoder.MaxMessageSize) {
            compressedPayload.release();
            String compressedStr = (!isBatchMessagingEnabled() && conf.getCompressionType() != CompressionType.NONE)
                    ? "Compressed"
                    : "";
            callback.sendComplete(new PulsarClientException.InvalidMessageException(
                    format("%s Message payload size %d cannot exceed %d bytes", compressedStr, compressedSize,
                            PulsarDecoder.MaxMessageSize)));
            return;
        }

        if (!msg.isReplicated() && msgMetadata.hasProducerName()) {
            callback.sendComplete(new PulsarClientException.InvalidMessageException("Cannot re-use the same message"));
            compressedPayload.release();
            return;
        }

        try {
            synchronized (this) {
                long sequenceId;
                if (!msgMetadata.hasSequenceId()) {
                    sequenceId = msgIdGeneratorUpdater.getAndIncrement(this);
                    msgMetadata.setSequenceId(sequenceId);
                } else {
                    sequenceId = msgMetadata.getSequenceId();
                }
                if (!msgMetadata.hasPublishTime()) {
                    msgMetadata.setPublishTime(System.currentTimeMillis());

                    checkArgument(!msgMetadata.hasProducerName());

                    msgMetadata.setProducerName(producerName);

                    if (conf.getCompressionType() != CompressionType.NONE) {
                        msgMetadata.setCompression(convertCompressionType(conf.getCompressionType()));
                        msgMetadata.setUncompressedSize(uncompressedSize);
                    }
                }

                if (isBatchMessagingEnabled()) {
                    // handle boundary cases where message being added would exceed
                    // batch size and/or max message size
                    if (batchMessageContainer.hasSpaceInBatch(msg)) {
                        batchMessageContainer.add(msg, callback);
                        payload.release();
                        if (batchMessageContainer.numMessagesInBatch == maxNumMessagesInBatch
                                || batchMessageContainer.currentBatchSizeBytes >= BatchMessageContainer.MAX_MESSAGE_BATCH_SIZE_BYTES) {
                            batchMessageAndSend();
                        }
                    } else {
                        doBatchSendAndAdd(msg, callback, payload);
                    }
                } else {
                    ByteBuf encryptedPayload = encryptMessage(msgMetadata, compressedPayload);
                    ByteBufPair cmd = sendMessage(producerId, sequenceId, 1, msgMetadata.build(), encryptedPayload);
                    msgMetadata.recycle();

                    final OpSendMsg op = OpSendMsg.create(msg, cmd, sequenceId, callback);
                    op.setNumMessagesInBatch(1);
                    op.setBatchSizeByte(encryptedPayload.readableBytes());
                    pendingMessages.put(op);

                    // Read the connection before validating if it's still connected, so that we avoid reading a null
                    // value
                    ClientCnx cnx = cnx();
                    if (isConnected()) {
                        // If we do have a connection, the message is sent immediately, otherwise we'll try again once a
                        // new
                        // connection is established
                        cmd.retain();
                        cnx.ctx().channel().eventLoop().execute(WriteInEventLoopCallback.create(this, cnx, op));
                        stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] [{}] Connection is not ready -- sequenceId {}", topic, producerName,
                                    sequenceId);
                        }
                    }
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            semaphore.release();
            callback.sendComplete(new PulsarClientException(ie));
        } catch (PulsarClientException e) {
            semaphore.release();
            callback.sendComplete(e);
        } catch (Throwable t) {
            semaphore.release();
            callback.sendComplete(new PulsarClientException(t));
        }
    }

    private ByteBuf encryptMessage(MessageMetadata.Builder msgMetadata, ByteBuf compressedPayload)
            throws PulsarClientException {

        ByteBuf encryptedPayload = compressedPayload;
        if (!conf.isEncryptionEnabled() || msgCrypto == null) {
            return encryptedPayload;
        }
        try {
            encryptedPayload = msgCrypto.encrypt(conf.getEncryptionKeys(), conf.getCryptoKeyReader(), msgMetadata,
                    compressedPayload);
        } catch (PulsarClientException e) {
            // Unless config is set to explicitly publish un-encrypted message upon failure, fail the request
            if (conf.getCryptoFailureAction() == ProducerCryptoFailureAction.SEND) {
                log.warn("[{}] [{}] Failed to encrypt message {}. Proceeding with publishing unencrypted message",
                        topic, producerName, e.getMessage());
                return compressedPayload;
            }
            throw e;
        }
        return encryptedPayload;
    }

    private ByteBufPair sendMessage(long producerId, long sequenceId, int numMessages, MessageMetadata msgMetadata,
            ByteBuf compressedPayload) throws IOException {
        ChecksumType checksumType;

        if (connectionHandler.getClientCnx() == null
                || connectionHandler.getClientCnx().getRemoteEndpointProtocolVersion() >= brokerChecksumSupportedVersion()) {
            checksumType = ChecksumType.Crc32c;
        } else {
            checksumType = ChecksumType.None;
        }
        return Commands.newSend(producerId, sequenceId, numMessages, checksumType, msgMetadata, compressedPayload);
    }

    private void doBatchSendAndAdd(MessageImpl<T> msg, SendCallback callback, ByteBuf payload) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Closing out batch to accomodate large message with size {}", topic, producerName,
                    msg.getDataBuffer().readableBytes());
        }
        batchMessageAndSend();
        batchMessageContainer.add(msg, callback);
        payload.release();
    }

    private boolean isValidProducerState(SendCallback callback) {
        switch (getState()) {
        case Ready:
            // OK
        case Connecting:
            // We are OK to queue the messages on the client, it will be sent to the broker once we get the connection
            return true;
        case Closing:
        case Closed:
            callback.sendComplete(new PulsarClientException.AlreadyClosedException("Producer already closed"));
            return false;
        case Terminated:
            callback.sendComplete(new PulsarClientException.TopicTerminatedException("Topic was terminated"));
            return false;
        case Failed:
        case Uninitialized:
        default:
            callback.sendComplete(new PulsarClientException.NotConnectedException());
            return false;
        }
    }

    private boolean canEnqueueRequest(SendCallback callback) {
        try {
            if (conf.isBlockIfQueueFull()) {
                semaphore.acquire();
            } else {
                if (!semaphore.tryAcquire()) {
                    callback.sendComplete(
                            new PulsarClientException.ProducerQueueIsFullError("Producer send queue is full"));
                    return false;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            callback.sendComplete(new PulsarClientException(e));
            return false;
        }

        return true;
    }

    private static final class WriteInEventLoopCallback implements Runnable {
        private ProducerImpl<?> producer;
        private ByteBufPair cmd;
        private long sequenceId;
        private ClientCnx cnx;

        static WriteInEventLoopCallback create(ProducerImpl<?> producer, ClientCnx cnx, OpSendMsg op) {
            WriteInEventLoopCallback c = RECYCLER.get();
            c.producer = producer;
            c.cnx = cnx;
            c.sequenceId = op.sequenceId;
            c.cmd = op.cmd;
            return c;
        }

        @Override
        public void run() {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Sending message cnx {}, sequenceId {}", producer.topic, producer.producerName, cnx,
                        sequenceId);
            }

            try {
                cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
            } finally {
                recycle();
            }
        }

        private void recycle() {
            producer = null;
            cnx = null;
            cmd = null;
            sequenceId = -1;
            recyclerHandle.recycle(this);
        }

        private final Handle<WriteInEventLoopCallback> recyclerHandle;

        private WriteInEventLoopCallback(Handle<WriteInEventLoopCallback> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<WriteInEventLoopCallback> RECYCLER = new Recycler<WriteInEventLoopCallback>() {
            @Override
            protected WriteInEventLoopCallback newObject(Handle<WriteInEventLoopCallback> handle) {
                return new WriteInEventLoopCallback(handle);
            }
        };
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        final State currentState = getAndUpdateState(state -> {
            if (state == State.Closed) {
                return state;
            }
            return State.Closing;
        });

        if (currentState == State.Closed || currentState == State.Closing) {
            return CompletableFuture.completedFuture(null);
        }

        Timeout timeout = sendTimeout;
        if (timeout != null) {
            timeout.cancel();
            sendTimeout = null;
        }

        Timeout batchTimeout = batchMessageAndSendTimeout;
        if (batchTimeout != null) {
            batchTimeout.cancel();
            batchMessageAndSendTimeout = null;
        }

        if (keyGeneratorTask != null && !keyGeneratorTask.isCancelled()) {
            keyGeneratorTask.cancel(false);
        }

        stats.cancelStatsTimeout();

        ClientCnx cnx = cnx();
        if (cnx == null || currentState != State.Ready) {
            log.info("[{}] [{}] Closed Producer (not connected)", topic, producerName);
            synchronized (this) {
                setState(State.Closed);
                client.cleanupProducer(this);
                PulsarClientException ex = new PulsarClientException.AlreadyClosedException(
                        "Producer was already closed");
                pendingMessages.forEach(msg -> {
                    msg.callback.sendComplete(ex);
                    msg.cmd.release();
                    msg.recycle();
                });
                pendingMessages.clear();
            }

            return CompletableFuture.completedFuture(null);
        }

        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newCloseProducer(producerId, requestId);

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
            cnx.removeProducer(producerId);
            if (exception == null || !cnx.ctx().channel().isActive()) {
                // Either we've received the success response for the close producer command from the broker, or the
                // connection did break in the meantime. In any case, the producer is gone.
                synchronized (ProducerImpl.this) {
                    log.info("[{}] [{}] Closed Producer", topic, producerName);
                    setState(State.Closed);
                    pendingMessages.forEach(msg -> {
                        msg.cmd.release();
                        msg.recycle();
                    });
                    pendingMessages.clear();
                }

                closeFuture.complete(null);
                client.cleanupProducer(this);
            } else {
                closeFuture.completeExceptionally(exception);
            }

            return null;
        });

        return closeFuture;
    }

    @Override
    public boolean isConnected() {
        return connectionHandler.getClientCnx() != null && (getState() == State.Ready);
    }

    public boolean isWritable() {
        ClientCnx cnx = connectionHandler.getClientCnx();
        return cnx != null && cnx.channel().isWritable();
    }

    public void terminated(ClientCnx cnx) {
        State previousState = getAndUpdateState(state -> (state == State.Closed ? State.Closed : State.Terminated));
        if (previousState != State.Terminated && previousState != State.Closed) {
            log.info("[{}] [{}] The topic has been terminated", topic, producerName);
            setClientCnx(null);

            failPendingMessages(cnx,
                    new PulsarClientException.TopicTerminatedException("The topic has been terminated"));
        }
    }

    void ackReceived(ClientCnx cnx, long sequenceId, long ledgerId, long entryId) {
        OpSendMsg op = null;
        boolean callback = false;
        synchronized (this) {
            op = pendingMessages.peek();
            if (op == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg {}", topic, producerName, sequenceId);
                }
                return;
            }

            long expectedSequenceId = op.sequenceId;
            if (sequenceId > expectedSequenceId) {
                log.warn("[{}] [{}] Got ack for msg. expecting: {} - got: {} - queue-size: {}", topic, producerName,
                        expectedSequenceId, sequenceId, pendingMessages.size());
                // Force connection closing so that messages can be retransmitted in a new connection
                cnx.channel().close();
            } else if (sequenceId < expectedSequenceId) {
                // Ignoring the ack since it's referring to a message that has already timed out.
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg {} last-seq: {}", topic, producerName, sequenceId,
                            expectedSequenceId);
                }
            } else {
                // Message was persisted correctly
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Received ack for msg {} ", topic, producerName, sequenceId);
                }
                pendingMessages.remove();
                semaphore.release(op.numMessagesInBatch);
                callback = true;
                pendingCallbacks.add(op);
            }
        }
        if (callback) {
            op = pendingCallbacks.poll();
            if (op != null) {
                lastSequenceIdPublished = op.sequenceId + op.numMessagesInBatch - 1;
                op.setMessageId(ledgerId, entryId, partitionIndex);
                try {
                    // Need to protect ourselves from any exception being thrown in the future handler from the
                    // application
                    op.callback.sendComplete(null);
                } catch (Throwable t) {
                    log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic, producerName,
                            sequenceId, t);
                }
                ReferenceCountUtil.safeRelease(op.cmd);
                op.recycle();
            }
        }
    }

    /**
     * Checks message checksum to retry if message was corrupted while sending to broker. Recomputes checksum of the
     * message header-payload again.
     * <ul>
     * <li><b>if matches with existing checksum</b>: it means message was corrupt while sending to broker. So, resend
     * message</li>
     * <li><b>if doesn't match with existing checksum</b>: it means message is already corrupt and can't retry again.
     * So, fail send-message by failing callback</li>
     * </ul>
     *
     * @param cnx
     * @param sequenceId
     */
    protected synchronized void recoverChecksumError(ClientCnx cnx, long sequenceId) {
        OpSendMsg op = pendingMessages.peek();
        if (op == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Got send failure for timed out msg {}", topic, producerName, sequenceId);
            }
        } else {
            long expectedSequenceId = op.sequenceId;
            if (sequenceId == expectedSequenceId) {
                boolean corrupted = !verifyLocalBufferIsNotCorrupted(op);
                if (corrupted) {
                    // remove message from pendingMessages queue and fail callback
                    pendingMessages.remove();
                    semaphore.release(op.numMessagesInBatch);
                    try {
                        op.callback.sendComplete(
                                new PulsarClientException.ChecksumException("Checksum failded on corrupt message"));
                    } catch (Throwable t) {
                        log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic,
                                producerName, sequenceId, t);
                    }
                    ReferenceCountUtil.safeRelease(op.cmd);
                    op.recycle();
                    return;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Message is not corrupted, retry send-message with sequenceId {}", topic,
                                producerName, sequenceId);
                    }
                }

            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Corrupt message is already timed out {}", topic, producerName, sequenceId);
                }
            }
        }
        // as msg is not corrupted : let producer resend pending-messages again including checksum failed message
        resendMessages(cnx);
    }

    /**
     * Computes checksum again and verifies it against existing checksum. If checksum doesn't match it means that
     * message is corrupt.
     *
     * @param op
     * @return returns true only if message is not modified and computed-checksum is same as previous checksum else
     *         return false that means that message is corrupted. Returns true if checksum is not present.
     */
    protected boolean verifyLocalBufferIsNotCorrupted(OpSendMsg op) {
        ByteBufPair msg = op.cmd;

        if (msg != null) {
            ByteBuf headerFrame = msg.getFirst();
            headerFrame.markReaderIndex();
            try {
                // skip bytes up to checksum index
                headerFrame.skipBytes(4); // skip [total-size]
                int cmdSize = (int) headerFrame.readUnsignedInt();
                headerFrame.skipBytes(cmdSize);
                // verify if checksum present
                if (hasChecksum(headerFrame)) {
                    int checksum = readChecksum(headerFrame).intValue();
                    // msg.readerIndex is already at header-payload index, Recompute checksum for headers-payload
                    int metadataChecksum = computeChecksum(headerFrame);
                    long computedChecksum = resumeChecksum(metadataChecksum, msg.getSecond());
                    return checksum == computedChecksum;
                } else {
                    log.warn("[{}] [{}] checksum is not present into message with id {}", topic, producerName,
                            op.sequenceId);
                }
            } finally {
                headerFrame.resetReaderIndex();
            }
            return true;
        } else {
            log.warn("[{}] Failed while casting {} into ByteBufPair", producerName, op.cmd.getClass().getName());
            return false;
        }
    }

    protected static final class OpSendMsg {
        MessageImpl<?> msg;
        List<MessageImpl<?>> msgs;
        ByteBufPair cmd;
        SendCallback callback;
        long sequenceId;
        long createdAt;
        long batchSizeByte = 0;
        int numMessagesInBatch = 1;

        static OpSendMsg create(MessageImpl<?> msg, ByteBufPair cmd, long sequenceId, SendCallback callback) {
            OpSendMsg op = RECYCLER.get();
            op.msg = msg;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = sequenceId;
            op.createdAt = System.currentTimeMillis();
            return op;
        }

        static OpSendMsg create(List<MessageImpl<?>> msgs, ByteBufPair cmd, long sequenceId, SendCallback callback) {
            OpSendMsg op = RECYCLER.get();
            op.msgs = msgs;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = sequenceId;
            op.createdAt = System.currentTimeMillis();
            return op;
        }

        void recycle() {
            msg = null;
            msgs = null;
            cmd = null;
            callback = null;
            sequenceId = -1;
            createdAt = -1;
            recyclerHandle.recycle(this);
        }

        void setNumMessagesInBatch(int numMessagesInBatch) {
            this.numMessagesInBatch = numMessagesInBatch;
        }

        void setBatchSizeByte(long batchSizeByte) {
            this.batchSizeByte = batchSizeByte;
        }

        void setMessageId(long ledgerId, long entryId, int partitionIndex) {
            if (msg != null) {
                msg.setMessageId(new MessageIdImpl(ledgerId, entryId, partitionIndex));
            } else {
                for (int batchIndex = 0; batchIndex < msgs.size(); batchIndex++) {
                    msgs.get(batchIndex)
                            .setMessageId(new BatchMessageIdImpl(ledgerId, entryId, partitionIndex, batchIndex));
                }
            }
        }

        private OpSendMsg(Handle<OpSendMsg> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private final Handle<OpSendMsg> recyclerHandle;
        private static final Recycler<OpSendMsg> RECYCLER = new Recycler<OpSendMsg>() {
            @Override
            protected OpSendMsg newObject(Handle<OpSendMsg> handle) {
                return new OpSendMsg(handle);
            }
        };
    }

    @Override
    public void connectionOpened(final ClientCnx cnx) {
        // we set the cnx reference before registering the producer on the cnx, so if the cnx breaks before creating the
        // producer, it will try to grab a new cnx
        connectionHandler.setClientCnx(cnx);
        cnx.registerProducer(producerId, this);

        log.info("[{}] [{}] Creating producer on cnx {}", topic, producerName, cnx.ctx().channel());

        long requestId = client.newRequestId();

        cnx.sendRequestWithId(
                Commands.newProducer(topic, producerId, requestId, producerName, conf.isEncryptionEnabled(), metadata),
                requestId).thenAccept(pair -> {
                    String producerName = pair.getLeft();
                    long lastSequenceId = pair.getRight();

                    // We are now reconnected to broker and clear to send messages. Re-send all pending messages and
                    // set the cnx pointer so that new messages will be sent immediately
                    synchronized (ProducerImpl.this) {
                        if (getState() == State.Closing || getState() == State.Closed) {
                            // Producer was closed while reconnecting, close the connection to make sure the broker
                            // drops the producer on its side
                            cnx.removeProducer(producerId);
                            cnx.channel().close();
                            return;
                        }
                        resetBackoff();

                        log.info("[{}] [{}] Created producer on cnx {}", topic, producerName, cnx.ctx().channel());
                        connectionId = cnx.ctx().channel().toString();
                        connectedSince = DateFormatter.now();

                        if (this.producerName == null) {
                            this.producerName = producerName;
                        }

                        if (this.lastSequenceIdPublished == -1 && conf.getInitialSequenceId() == null) {
                            this.lastSequenceIdPublished = lastSequenceId;
                            this.msgIdGenerator = lastSequenceId + 1;
                        }

                        if (!producerCreatedFuture.isDone() && isBatchMessagingEnabled()) {
                            // schedule the first batch message task
                            client.timer().newTimeout(batchMessageAndSendTask, conf.getBatchingMaxPublishDelayMicros(),
                                    TimeUnit.MICROSECONDS);
                        }
                        resendMessages(cnx);
                    }
                }).exceptionally((e) -> {
                    Throwable cause = e.getCause();
                    cnx.removeProducer(producerId);
                    if (getState() == State.Closing || getState() == State.Closed) {
                        // Producer was closed while reconnecting, close the connection to make sure the broker
                        // drops the producer on its side
                        cnx.channel().close();
                        return null;
                    }
                    log.error("[{}] [{}] Failed to create producer: {}", topic, producerName, cause.getMessage());

                    if (cause instanceof PulsarClientException.ProducerBlockedQuotaExceededException) {
                        synchronized (this) {
                            log.warn("[{}] [{}] Topic backlog quota exceeded. Throwing Exception on producer.", topic,
                                    producerName);

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] [{}] Pending messages: {}", topic, producerName,
                                        pendingMessages.size());
                            }

                            PulsarClientException bqe = new PulsarClientException.ProducerBlockedQuotaExceededException(
                                    "Could not send pending messages as backlog exceeded");
                            failPendingMessages(cnx(), bqe);
                        }
                    } else if (cause instanceof PulsarClientException.ProducerBlockedQuotaExceededError) {
                        log.warn("[{}] [{}] Producer is blocked on creation because backlog exceeded on topic.",
                                producerName, topic);
                    }

                    if (cause instanceof PulsarClientException.TopicTerminatedException) {
                        setState(State.Terminated);
                        failPendingMessages(cnx(), (PulsarClientException) cause);
                        producerCreatedFuture.completeExceptionally(cause);
                        client.cleanupProducer(this);
                    } else if (producerCreatedFuture.isDone() || //
                    (cause instanceof PulsarClientException && connectionHandler.isRetriableError((PulsarClientException) cause)
                            && System.currentTimeMillis() < createProducerTimeout)) {
                        // Either we had already created the producer once (producerCreatedFuture.isDone()) or we are
                        // still within the initial timeout budget and we are dealing with a retriable error
                        reconnectLater(cause);
                    } else {
                        setState(State.Failed);
                        producerCreatedFuture.completeExceptionally(cause);
                        client.cleanupProducer(this);
                    }

                    return null;
                });
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        if (System.currentTimeMillis() > createProducerTimeout
                && producerCreatedFuture.completeExceptionally(exception)) {
            log.info("[{}] Producer creation failed for producer {}", topic, producerId);
            setState(State.Failed);
            client.cleanupProducer(this);
        }
    }

    private void resendMessages(ClientCnx cnx) {
        cnx.ctx().channel().eventLoop().execute(() -> {
            synchronized (this) {
                if (getState() == State.Closing || getState() == State.Closed) {
                    // Producer was closed while reconnecting, close the connection to make sure the broker
                    // drops the producer on its side
                    cnx.channel().close();
                    return;
                }
                int messagesToResend = pendingMessages.size();
                if (messagesToResend == 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] No pending messages to resend {}", topic, producerName, messagesToResend);
                    }
                    if (changeToReadyState()) {
                        producerCreatedFuture.complete(ProducerImpl.this);
                        return;
                    } else {
                        // Producer was closed while reconnecting, close the connection to make sure the broker
                        // drops the producer on its side
                        cnx.channel().close();
                        return;
                    }

                }

                log.info("[{}] [{}] Re-Sending {} messages to server", topic, producerName, messagesToResend);

                final boolean stripChecksum = cnx.getRemoteEndpointProtocolVersion() < brokerChecksumSupportedVersion();
                for (OpSendMsg op : pendingMessages) {

                    if (stripChecksum) {
                        stripChecksum(op);
                    }
                    op.cmd.retain();
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Re-Sending message in cnx {}, sequenceId {}", topic, producerName,
                                cnx.channel(), op.sequenceId);
                    }
                    cnx.ctx().write(op.cmd, cnx.ctx().voidPromise());
                    stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
                }

                cnx.ctx().flush();
                if (!changeToReadyState()) {
                    // Producer was closed while reconnecting, close the connection to make sure the broker
                    // drops the producer on its side
                    cnx.channel().close();
                    return;
                }
            }
        });
    }

    /**
     * Strips checksum from {@link OpSendMsg} command if present else ignore it.
     *
     * @param op
     */
    private void stripChecksum(OpSendMsg op) {
        int totalMsgBufSize = op.cmd.readableBytes();
        ByteBufPair msg = op.cmd;
        if (msg != null) {
            ByteBuf headerFrame = msg.getFirst();
            headerFrame.markReaderIndex();
            try {
                headerFrame.skipBytes(4); // skip [total-size]
                int cmdSize = (int) headerFrame.readUnsignedInt();

                // verify if checksum present
                headerFrame.skipBytes(cmdSize);

                if (!hasChecksum(headerFrame)) {
                    return;
                }

                int headerSize = 4 + 4 + cmdSize; // [total-size] [cmd-length] [cmd-size]
                int checksumSize = 4 + 2; // [magic-number] [checksum-size]
                int checksumMark = (headerSize + checksumSize); // [header-size] [checksum-size]
                int metaPayloadSize = (totalMsgBufSize - checksumMark); // metadataPayload = totalSize - checksumMark
                int newTotalFrameSizeLength = 4 + cmdSize + metaPayloadSize; // new total-size without checksum
                headerFrame.resetReaderIndex();
                int headerFrameSize = headerFrame.readableBytes();

                headerFrame.setInt(0, newTotalFrameSizeLength); // rewrite new [total-size]
                ByteBuf metadata = headerFrame.slice(checksumMark, headerFrameSize - checksumMark); // sliced only
                                                                                                    // metadata
                headerFrame.writerIndex(headerSize); // set headerFrame write-index to overwrite metadata over checksum
                metadata.readBytes(headerFrame, metadata.readableBytes());
                headerFrame.capacity(headerFrameSize - checksumSize); // reduce capacity by removed checksum bytes
            } finally {
                headerFrame.resetReaderIndex();
            }
        } else {
            log.warn("[{}] Failed while casting {} into ByteBufPair", producerName, op.cmd.getClass().getName());
        }
    }

    public int brokerChecksumSupportedVersion() {
        return ProtocolVersion.v6.getNumber();
    }

    @Override
    String getHandlerName() {
        return producerName;
    }

    /**
     * Process sendTimeout events
     */
    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }

        long timeToWaitMs;

        synchronized (this) {
            OpSendMsg firstMsg = pendingMessages.peek();
            if (firstMsg == null) {
                // If there are no pending messages, reset the timeout to the configured value.
                timeToWaitMs = conf.getSendTimeoutMs();
            } else {
                // If there is at least one message, calculate the diff between the message timeout and the current
                // time.
                long diff = (firstMsg.createdAt + conf.getSendTimeoutMs()) - System.currentTimeMillis();
                if (diff <= 0) {
                    // The diff is less than or equal to zero, meaning that the message has been timed out.
                    // Set the callback to timeout on every message, then clear the pending queue.
                    log.info("[{}] [{}] Message send timed out. Failing {} messages", topic, producerName,
                            pendingMessages.size());

                    PulsarClientException te = new PulsarClientException.TimeoutException(
                            "Could not send message to broker within given timeout");
                    failPendingMessages(cnx(), te);
                    stats.incrementSendFailed(pendingMessages.size());
                    // Since the pending queue is cleared now, set timer to expire after configured value.
                    timeToWaitMs = conf.getSendTimeoutMs();
                } else {
                    // The diff is greater than zero, set the timeout to the diff value
                    timeToWaitMs = diff;
                }
            }
        }

        sendTimeout = client.timer().newTimeout(this, timeToWaitMs, TimeUnit.MILLISECONDS);
    }

    /**
     * This fails and clears the pending messages with the given exception. This method should be called from within the
     * ProducerImpl object mutex.
     */
    private void failPendingMessages(ClientCnx cnx, PulsarClientException ex) {
        if (cnx == null) {
            final AtomicInteger releaseCount = new AtomicInteger();
            pendingMessages.forEach(op -> {
                releaseCount.addAndGet(op.numMessagesInBatch);
                try {
                    // Need to protect ourselves from any exception being thrown in the future handler from the
                    // application
                    op.callback.sendComplete(ex);
                } catch (Throwable t) {
                    log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic, producerName,
                            op.sequenceId, t);
                }
                ReferenceCountUtil.safeRelease(op.cmd);
                op.recycle();
            });
            semaphore.release(releaseCount.get());
            pendingMessages.clear();
            pendingCallbacks.clear();
            if (isBatchMessagingEnabled()) {
                failPendingBatchMessages(ex);
            }
        } else {
            // If we have a connection, we schedule the callback and recycle on the event loop thread to avoid any
            // race condition since we also write the message on the socket from this thread
            cnx.ctx().channel().eventLoop().execute(() -> {
                synchronized (ProducerImpl.this) {
                    failPendingMessages(null, ex);
                }
            });
        }
    }

    /**
     * fail any pending batch messages that were enqueued, however batch was not closed out
     *
     */
    private void failPendingBatchMessages(PulsarClientException ex) {
        if (batchMessageContainer.isEmpty()) {
            return;
        }
        int numMessagesInBatch = batchMessageContainer.numMessagesInBatch;
        semaphore.release(numMessagesInBatch);
        try {
            // Need to protect ourselves from any exception being thrown in the future handler from the application
            batchMessageContainer.firstCallback.sendComplete(ex);
        } catch (Throwable t) {
            log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic, producerName,
                    batchMessageContainer.sequenceId, t);
        }
        ReferenceCountUtil.safeRelease(batchMessageContainer.getBatchedSingleMessageMetadataAndPayload());
        batchMessageContainer.clear();
    }

    TimerTask batchMessageAndSendTask = new TimerTask() {

        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Batching the messages from the batch container from timer thread", topic,
                        producerName);
            }
            // semaphore acquired when message was enqueued to container
            synchronized (ProducerImpl.this) {
                batchMessageAndSend();
            }
            // schedule the next batch message task
            batchMessageAndSendTimeout = client.timer()
                .newTimeout(this, conf.getBatchingMaxPublishDelayMicros(), TimeUnit.MICROSECONDS);
        }
    };

    @Override
    public MessageId send(Message<T> message) throws PulsarClientException {
        try {
            // enqueue the message to the buffer
            CompletableFuture<MessageId> sendFuture = sendAsync(message);

            if (!sendFuture.isDone()) {
                // the send request wasn't completed yet (e.g. not failing at enqueuing), then attempt to flush it out
                flush();
            }

            return sendFuture.get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    private void flush() {
        if (isBatchMessagingEnabled()) {
            synchronized (ProducerImpl.this) {
                batchMessageAndSend();
            }
        }
    }

    // must acquire semaphore before enqueuing
    private void batchMessageAndSend() {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Batching the messages from the batch container with {} messages", topic, producerName,
                    batchMessageContainer.numMessagesInBatch);
        }
        OpSendMsg op = null;
        int numMessagesInBatch = 0;
        try {
            if (!batchMessageContainer.isEmpty()) {
                numMessagesInBatch = batchMessageContainer.numMessagesInBatch;
                ByteBuf compressedPayload = batchMessageContainer.getCompressedBatchMetadataAndPayload();
                long sequenceId = batchMessageContainer.sequenceId;
                ByteBuf encryptedPayload = encryptMessage(batchMessageContainer.messageMetadata, compressedPayload);
                ByteBufPair cmd = sendMessage(producerId, sequenceId, batchMessageContainer.numMessagesInBatch,
                        batchMessageContainer.setBatchAndBuild(), encryptedPayload);

                op = OpSendMsg.create(batchMessageContainer.messages, cmd, sequenceId,
                        batchMessageContainer.firstCallback);

                op.setNumMessagesInBatch(batchMessageContainer.numMessagesInBatch);
                op.setBatchSizeByte(batchMessageContainer.currentBatchSizeBytes);

                batchMessageContainer.clear();

                pendingMessages.put(op);

                if (isConnected()) {
                    // If we do have a connection, the message is sent immediately, otherwise we'll try again once a new
                    // connection is established
                    cmd.retain();
                    cnx().ctx().channel().eventLoop().execute(WriteInEventLoopCallback.create(this, cnx(), op));
                    stats.updateNumMsgsSent(numMessagesInBatch, op.batchSizeByte);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Connection is not ready -- sequenceId {}", topic, producerName,
                                sequenceId);
                    }
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            semaphore.release(numMessagesInBatch);
            if (op != null) {
                op.callback.sendComplete(new PulsarClientException(ie));
            }
        } catch (PulsarClientException e) {
            Thread.currentThread().interrupt();
            semaphore.release(numMessagesInBatch);
            if (op != null) {
                op.callback.sendComplete(e);
            }
        } catch (Throwable t) {
            semaphore.release(numMessagesInBatch);
            log.warn("[{}] [{}] error while closing out batch -- {}", topic, producerName, t);
            if (op != null) {
                op.callback.sendComplete(new PulsarClientException(t));
            }
        }
    }

    public long getDelayInMillis() {
        OpSendMsg firstMsg = pendingMessages.peek();
        if (firstMsg != null) {
            return System.currentTimeMillis() - firstMsg.createdAt;
        }
        return 0L;
    }

    public String getConnectionId() {
        return cnx() != null ? connectionId : null;
    }

    public String getConnectedSince() {
        return cnx() != null ? connectedSince : null;
    }

    public int getPendingQueueSize() {
        return pendingMessages.size();
    }

    private PulsarApi.CompressionType convertCompressionType(CompressionType compressionType) {
        switch (compressionType) {
        case NONE:
            return PulsarApi.CompressionType.NONE;
        case LZ4:
            return PulsarApi.CompressionType.LZ4;
        case ZLIB:
            return PulsarApi.CompressionType.ZLIB;

        default:
            throw new RuntimeException("Invalid compression type");
        }
    }

    @Override
    public ProducerStatsRecorder getStats() {
        return stats;
    }

    public String getProducerName() {
        return producerName;
    }

    // wrapper for connection methods
    ClientCnx cnx() {
        return this.connectionHandler.cnx();
    }

    void resetBackoff() {
        this.connectionHandler.resetBackoff();
    }

    void connectionClosed(ClientCnx cnx) {
        this.connectionHandler.connectionClosed(cnx);
    }

    ClientCnx getClientCnx() {
        return this.connectionHandler.getClientCnx();
    }

    void setClientCnx(ClientCnx clientCnx) {
        this.connectionHandler.setClientCnx(clientCnx);
    }

    void reconnectLater(Throwable exception) {
        this.connectionHandler.reconnectLater(exception);
    }

    void grabCnx() {
        this.connectionHandler.grabCnx();
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerImpl.class);
}
