package org.jast.apps.gcp.pubsub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;

@Component
public class BucketSubscriber implements CommandLineRunner {

	private final BucketMessageReceiver messageReceiver;

	@Value("${org.jast.apps.gcp.pubsub.gpc-project-id}")
	private String projectId;
	@Value("${org.jast.apps.gcp.pubsub.gpc-subscription-id}")
	private String subscriptionId;

	public BucketSubscriber(BucketMessageReceiver messageReceiver) {
		this.messageReceiver = messageReceiver;
	}

	@Override
	public void run(String... args) throws Exception {
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

		Subscriber subscriber = null;
		try {
			subscriber = Subscriber.newBuilder(subscriptionName, this.messageReceiver).build();
			// Start the subscriber.
			subscriber.startAsync().awaitRunning();
			System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
			// Allow the subscriber to run for 30s unless an unrecoverable error occurs.
			subscriber.awaitTerminated();
		} catch (Exception timeoutException) {
			// Shut down the subscriber after 30s. Stop receiving messages.
			subscriber.stopAsync();
		}
	}

}
