package com.upplication.s3fs;

import java.net.URI;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class AmazonS3ClientFactory implements AmazonS3Factory {
	private Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public AmazonS3 getAmazonS3(URI uri, Properties props) {
		RequestMetricCollector requestMetricCollector = null;
		if(props.containsKey(REQUEST_METRIC_COLLECTOR_CLASS)) {
			try {
				requestMetricCollector = (RequestMetricCollector) Class.forName(props.getProperty(REQUEST_METRIC_COLLECTOR_CLASS)).newInstance();
			} catch (Throwable t) {
				logger.warn("Can't instantiate REQUEST_METRIC_COLLECTOR_CLASS "+props.getProperty(REQUEST_METRIC_COLLECTOR_CLASS), t);
			}
		}
		AmazonS3 client;
		if (props.getProperty(ACCESS_KEY) == null && props.getProperty(SECRET_KEY) == null)
			client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain(), getClientConfiguration(props), requestMetricCollector);
		else
			client = new AmazonS3Client(new StaticCredentialsProvider(getAWSCredentials(props)), getClientConfiguration(props), requestMetricCollector);

		if (uri.getHost() != null)
			client.setEndpoint(uri.getHost());
		return client;
	}
	
	protected ClientConfiguration getClientConfiguration(Properties props) {
		ClientConfiguration clientConfiguration = new ClientConfiguration();
		if(props.getProperty(CONNECTION_TIMEOUT) != null)
			clientConfiguration.setConnectionTimeout(Integer.parseInt(props.getProperty(CONNECTION_TIMEOUT)));
		if(props.getProperty(MAX_CONNECTIONS) != null)
			clientConfiguration.setMaxConnections(Integer.parseInt(props.getProperty(MAX_CONNECTIONS)));
		if(props.getProperty(MAX_RETRY_ERROR) != null)
			clientConfiguration.setMaxErrorRetry(Integer.parseInt(props.getProperty(MAX_RETRY_ERROR)));
		if(props.getProperty(PROTOCOL) != null)
			clientConfiguration.setProtocol(Protocol.valueOf(props.getProperty(PROTOCOL)));
		if(props.getProperty(PROXY_DOMAIN) != null)
			clientConfiguration.setProxyDomain(props.getProperty(PROXY_DOMAIN));
		if(props.getProperty(PROXY_HOST) != null)
			clientConfiguration.setProxyHost(props.getProperty(PROXY_HOST));
		if(props.getProperty(PROXY_PASSWORD) != null)
			clientConfiguration.setProxyPassword(props.getProperty(PROXY_PASSWORD));
		if(props.getProperty(PROXY_PORT) != null)
			clientConfiguration.setProxyPort(Integer.parseInt(props.getProperty(PROXY_PORT)));
		if(props.getProperty(PROXY_USERNAME) != null)
			clientConfiguration.setProxyUsername(props.getProperty(PROXY_USERNAME));
		if(props.getProperty(PROXY_WORKSTATION) != null)
			clientConfiguration.setProxyWorkstation(props.getProperty(PROXY_WORKSTATION));
		if(props.getProperty(SOCKET_SEND_BUFFER_SIZE_HINT) != null || props.getProperty(SOCKET_RECEIVE_BUFFER_SIZE_HINT) != null) {
			int socketSendBufferSizeHint = props.getProperty(SOCKET_SEND_BUFFER_SIZE_HINT) == null ? 0 : Integer.parseInt(props.getProperty(SOCKET_SEND_BUFFER_SIZE_HINT));
			int socketReceiveBufferSizeHint = props.getProperty(SOCKET_RECEIVE_BUFFER_SIZE_HINT) == null ? 0 : Integer.parseInt(props.getProperty(SOCKET_RECEIVE_BUFFER_SIZE_HINT));
			clientConfiguration.setSocketBufferSizeHints(socketSendBufferSizeHint, socketReceiveBufferSizeHint);
		}
		if(props.getProperty(SOCKET_TIMEOUT) != null)
			clientConfiguration.setSocketTimeout(Integer.parseInt(props.getProperty(SOCKET_TIMEOUT)));
		if(props.getProperty(USER_AGENT) != null)
			clientConfiguration.setUserAgent(props.getProperty(USER_AGENT));
		return clientConfiguration;
	}

	protected BasicAWSCredentials getAWSCredentials(Properties props) {
		return new BasicAWSCredentials(props.getProperty(ACCESS_KEY), props.getProperty(SECRET_KEY));
	}
}