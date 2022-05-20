package org.jast.apps.gcp.pubsub.services;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.jast.apps.gcp.pubsub.dtos.BucketEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

@Service
public class FileDownloadService {

	private final Logger LOG = LoggerFactory.getLogger(FileDownloadService.class);

	@Value("${org.jast.apps.gcp.pubsub.gpc-project-id}")
	private String projectId;
	@Value("${org.jast.apps.gcp.pubsub.local-path}")
	private String localPath;

	public void downloadFile(BucketEvent bucketEvent) {
		Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
		Blob blob = storage.get(BlobId.of(bucketEvent.getBucket(), bucketEvent.getName()));
		if (blob != null) {
			Path path = Paths.get(localPath, bucketEvent.getName());
			blob.downloadTo(path);

			LOG.info("Imagen {} creada con exito en {}", bucketEvent.getName(), localPath);
		}
	}

}
