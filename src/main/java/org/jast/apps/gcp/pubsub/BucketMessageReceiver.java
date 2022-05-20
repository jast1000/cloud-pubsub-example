package org.jast.apps.gcp.pubsub;

import org.jast.apps.gcp.pubsub.dtos.BucketEvent;
import org.jast.apps.gcp.pubsub.services.FileDownloadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.gson.Gson;
import com.google.pubsub.v1.PubsubMessage;

@Component
public class BucketMessageReceiver implements MessageReceiver {

	private final Logger LOG = LoggerFactory.getLogger(BucketMessageReceiver.class);
	private final Gson GSON = new Gson();
	
	private final FileDownloadService service;
	
	public BucketMessageReceiver(FileDownloadService service) {
		this.service = service;
	}
	
	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		LOG.info(message.getData().toStringUtf8());
		
		var bucketEvent = GSON.fromJson(message.getData().toStringUtf8(), BucketEvent.class);
		service.downloadFile(bucketEvent);
		
		consumer.ack();
	}

}
