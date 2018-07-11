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
package org.apache.pulsar.broker.offload.impl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.offload.BlockAwareSegmentInputStream;
import org.apache.pulsar.broker.offload.OffloadIndexBlock;
import org.apache.pulsar.broker.offload.OffloadIndexBlockBuilder;
import org.apache.pulsar.utils.PulsarBrokerVersionStringUtils;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.jclouds.s3.reference.S3Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerOffloader implements LedgerOffloader {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerOffloader.class);

    public static final String[] DRIVER_NAMES = {"S3", "aws-s3", "google-cloud-storage"};

    static final String METADATA_FORMAT_VERSION_KEY = "S3ManagedLedgerOffloaderFormatVersion";
    static final String METADATA_SOFTWARE_VERSION_KEY = "S3ManagedLedgerOffloaderSoftwareVersion";
    static final String METADATA_SOFTWARE_GITSHA_KEY = "S3ManagedLedgerOffloaderSoftwareGitSha";
    static final String CURRENT_VERSION = String.valueOf(1);

    public static boolean driverSupported(String driver) {
        return Arrays.stream(DRIVER_NAMES).anyMatch(d -> d.equalsIgnoreCase(driver));
    }

    private static void addVersionInfo(BlobBuilder blobBuilder) {
        blobBuilder.userMetadata(ImmutableMap.of(
            METADATA_FORMAT_VERSION_KEY, CURRENT_VERSION,
            METADATA_SOFTWARE_VERSION_KEY, PulsarBrokerVersionStringUtils.getNormalizedVersionString(),
            METADATA_SOFTWARE_GITSHA_KEY, PulsarBrokerVersionStringUtils.getGitSha()));
    }

    private final VersionCheck VERSION_CHECK = (key, blob) -> {
        // NOTE all metadata in jclouds comes out as lowercase, in an effort to normalize the providers
        String version = blob.getMetadata().getUserMetadata().get(METADATA_FORMAT_VERSION_KEY.toLowerCase());
        if (version == null || !version.equals(CURRENT_VERSION)) {
            throw new IOException(String.format("Invalid object version %s for %s, expect %s",
                                                version, key, CURRENT_VERSION));
        }
    };

    private final OrderedScheduler scheduler;

    // container in jclouds
    private final String bucket;
    // max block size for each data block.
    private int maxBlockSize;
    private final int readBufferSize;

    private BlobStoreContext context;
    private BlobStore blobStore;
    Location location = null;

    public static ManagedLedgerOffloader create(ServiceConfiguration conf,
                                                OrderedScheduler scheduler)
            throws PulsarServerException {
        String driver = conf.getManagedLedgerOffloadDriver();
        String region = conf.getS3ManagedLedgerOffloadRegion();
        String bucket = conf.getS3ManagedLedgerOffloadBucket();
        String endpoint = conf.getS3ManagedLedgerOffloadServiceEndpoint();
        int maxBlockSize = conf.getS3ManagedLedgerOffloadMaxBlockSizeInBytes();
        int readBufferSize = conf.getS3ManagedLedgerOffloadReadBufferSizeInBytes();

        if (Strings.isNullOrEmpty(region) && Strings.isNullOrEmpty(endpoint)) {
            throw new PulsarServerException(
                    "Either s3ManagedLedgerOffloadRegion or s3ManagedLedgerOffloadServiceEndpoint must be set"
                    + " if s3 offload enabled");
        }
        if (Strings.isNullOrEmpty(bucket)) {
            throw new PulsarServerException("s3ManagedLedgerOffloadBucket cannot be empty if s3 offload enabled");
        }
        if (maxBlockSize < 5*1024*1024) {
            throw new PulsarServerException("s3ManagedLedgerOffloadMaxBlockSizeInBytes cannot be less than 5MB");
        }

        return new ManagedLedgerOffloader(driver, bucket, scheduler, maxBlockSize, readBufferSize, endpoint, region);
    }

    // build context for jclouds BlobStoreContext
    ManagedLedgerOffloader(String driver, String container, OrderedScheduler scheduler,
                           int maxBlockSize, int readBufferSize, String endpoint, String region) {
        this.scheduler = scheduler;
        this.readBufferSize = readBufferSize;

        this.bucket = container;
        this.maxBlockSize = maxBlockSize;

        Properties overrides = new Properties();
        // This property controls the number of parts being uploaded in parallel.
        overrides.setProperty("jclouds.mpu.parallel.degree", "1");
        overrides.setProperty("jclouds.mpu.parts.size", Integer.toString(maxBlockSize));
        overrides.setProperty(Constants.PROPERTY_SO_TIMEOUT, "25000");
        overrides.setProperty(Constants.PROPERTY_MAX_RETRIES, Integer.toString(100));

        ContextBuilder contextBuilder = ContextBuilder.newBuilder(driver);

        AWSCredentials credentials = null;
        try {
            DefaultAWSCredentialsProviderChain creds = DefaultAWSCredentialsProviderChain.getInstance();
            credentials = creds.getCredentials();
        } catch (Exception e) {
            log.error("Exception when get credentials for s3 ", e);
        }

        String id = "accesskey";
        String key = "secretkey";
        if (credentials != null) {
            id = credentials.getAWSAccessKeyId();
            key = credentials.getAWSSecretKey();
        }
        contextBuilder.credentials(id, key);

        if (!Strings.isNullOrEmpty(endpoint)) {
            contextBuilder.endpoint(endpoint);
            overrides.setProperty(S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false");
        }
        if (!Strings.isNullOrEmpty(region)) {
            this.location = new LocationBuilder().scope(LocationScope.REGION).id(region).description(region).build();
        }

        log.info("Constructor driver: {}, host: {}, [id: {}, key: {}], container: {}, region: {} ",
            driver, endpoint, id, key, bucket, region);

        contextBuilder.overrides(overrides);
        this.context = contextBuilder.buildView(BlobStoreContext.class);
        this.blobStore = context.getBlobStore();
    }

    // build context for jclouds BlobStoreContext
    ManagedLedgerOffloader(BlobStore blobStore, String container, OrderedScheduler scheduler,
                           int maxBlockSize, int readBufferSize) {
        this.scheduler = scheduler;
        this.readBufferSize = readBufferSize;
        this.bucket = container;
        this.maxBlockSize = maxBlockSize;
        this.blobStore = blobStore;
    }

    static String dataBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("%s-ledger-%d", uuid.toString(), ledgerId);
    }

    static String indexBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("%s-ledger-%d-index", uuid.toString(), ledgerId);
    }

    public boolean createBucket() {
        return blobStore.createContainerInLocation(location, bucket);
    }

    // upload DataBlock to s3 using MultiPartUpload, and indexBlock in a new Block,
    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle,
                                           UUID uuid,
                                           Map<String, String> extraMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(readHandle.getId()).submit(() -> {
            if (readHandle.getLength() == 0 || !readHandle.isClosed() || readHandle.getLastAddConfirmed() < 0) {
                promise.completeExceptionally(
                        new IllegalArgumentException("An empty or open ledger should never be offloaded"));
                return;
            }
            OffloadIndexBlockBuilder indexBuilder = OffloadIndexBlockBuilder.create()
                .withLedgerMetadata(readHandle.getLedgerMetadata())
                .withDataBlockHeaderLength(BlockAwareSegmentInputStreamImpl.getHeaderSize());
            String dataBlockKey = dataBlockOffloadKey(readHandle.getId(), uuid);
            String indexBlockKey = indexBlockOffloadKey(readHandle.getId(), uuid);

            MultipartUpload mpu = null;
            List<MultipartPart> parts = Lists.newArrayList();

            // init multi part upload for data block.
            try {
                BlobBuilder blobBuilder = blobStore.blobBuilder(dataBlockKey);
                addVersionInfo(blobBuilder);
                Blob blob = blobBuilder.build();
                mpu = blobStore.initiateMultipartUpload(bucket, blob.getMetadata(), new PutOptions());
            } catch (Throwable t) {
                promise.completeExceptionally(t);
                return;
            }

            long dataObjectLength = 0;
            // start multi part upload for data block.
            try {
                long startEntry = 0;
                int partId = 1;
                long entryBytesWritten = 0;
                while (startEntry <= readHandle.getLastAddConfirmed()) {
                    int blockSize = BlockAwareSegmentInputStreamImpl
                        .calculateBlockSize(maxBlockSize, readHandle, startEntry, entryBytesWritten);

                    try (BlockAwareSegmentInputStream blockStream = new BlockAwareSegmentInputStreamImpl(
                        readHandle, startEntry, blockSize)) {

                        Payload partPayload = Payloads.newInputStreamPayload(blockStream);
                        partPayload.getContentMetadata().setContentLength((long)blockSize);
                        partPayload.getContentMetadata().setContentType("text/plain");
                        parts.add(blobStore.uploadMultipartPart(mpu, partId, partPayload));
                        log.debug("UploadMultipartPart. container: {}, blobName: {}, partId: {}, mpu: {}",
                            bucket, dataBlockKey, partId, mpu.id());

                        indexBuilder.addBlock(startEntry, partId, blockSize);

                        if (blockStream.getEndEntryId() != -1) {
                            startEntry = blockStream.getEndEntryId() + 1;
                        } else {
                            // could not read entry from ledger.
                            break;
                        }
                        entryBytesWritten += blockStream.getBlockEntryBytesCount();
                        partId++;
                    }

                    dataObjectLength += blockSize;
                }

                blobStore.completeMultipartUpload(mpu, parts);
                mpu = null;
            } catch (Throwable t) {
                try {
                    if (mpu != null) {
                        blobStore.abortMultipartUpload(mpu);
                    }
                } catch (Throwable throwable) {
                    log.error("Failed abortMultipartUpload in bucket - {} with key - {}, uploadId - {}.",
                        bucket, dataBlockKey, mpu.id(), throwable);
                }
                promise.completeExceptionally(t);
                return;
            }

            // upload index block
            try (OffloadIndexBlock index = indexBuilder.withDataObjectLength(dataObjectLength).build();
                 OffloadIndexBlock.IndexInputStream indexStream = index.toStream()) {
                // write the index block
                BlobBuilder blobBuilder = blobStore.blobBuilder(indexBlockKey);
                addVersionInfo(blobBuilder);
                Payload indexPayload = Payloads.newInputStreamPayload(indexStream);
                indexPayload.getContentMetadata().setContentLength((long)indexStream.getStreamSize());
                indexPayload.getContentMetadata().setContentType("text/plain");

                Blob blob = blobBuilder
                    .payload(indexPayload)
                    .contentLength((long)indexStream.getStreamSize())
                    .build();

                blobStore.putBlob(bucket, blob);
                promise.complete(null);
            } catch (Throwable t) {
                try {
                    blobStore.removeBlob(bucket, dataBlockKey);
                } catch (Throwable throwable) {
                    log.error("Failed deleteObject in bucket - {} with key - {}.",
                        bucket, dataBlockKey, throwable);
                }
                promise.completeExceptionally(t);
                return;
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid) {
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String key = dataBlockOffloadKey(ledgerId, uid);
        String indexKey = indexBlockOffloadKey(ledgerId, uid);
        scheduler.chooseThread(ledgerId).submit(() -> {
                try {
                    promise.complete(BackedReadHandleImpl.open(scheduler.chooseThread(ledgerId),
                                                                 blobStore,
                                                                 bucket, key, indexKey,
                                                                 VERSION_CHECK,
                                                                 ledgerId, readBufferSize));
                } catch (Throwable t) {
                    log.error("Failed readOffloaded: ", t);
                    promise.completeExceptionally(t);
                }
            });
        return promise;
    }



    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                blobStore.removeBlob(bucket, dataBlockOffloadKey(ledgerId, uid));
                blobStore.removeBlob(bucket, indexBlockOffloadKey(ledgerId, uid));
                // adobe/s3mock not support removeBlobs well.
                /*blobStore.removeBlobs(bucket,
                    ImmutableList.of(dataBlockOffloadKey(ledgerId, uid), indexBlockOffloadKey(ledgerId, uid)));*/
                promise.complete(null);
            } catch (Throwable t) {
                log.error("Failed delete Blob", t);
                promise.completeExceptionally(t);
            }
        });

        return promise;
    }

    public interface VersionCheck {
        void check(String key, Blob blob) throws IOException;
    }
}

