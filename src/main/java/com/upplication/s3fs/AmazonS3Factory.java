package com.upplication.s3fs;

import java.net.URI;
import java.util.Properties;

import com.amazonaws.services.s3.AmazonS3;

public interface AmazonS3Factory {
	public static final String ACCESS_KEY = "access_key";
	public static final String SECRET_KEY = "secret_key";
	public static final String REQUEST_METRIC_COLLECTOR_CLASS = "request_metric_collector_class";
	public static final String CONNECTION_TIMEOUT = "s3fs_connection_timeout";
	public static final String MAX_CONNECTIONS = "s3fs_max_connections";
	public static final String MAX_RETRY_ERROR = "s3fs_max_retry_error";
	public static final String PROTOCOL = "s3fs_protocol";
	public static final String PROXY_DOMAIN = "s3fs_proxy_domain";
	public static final String PROXY_HOST = "s3fs_proxy_host";
	public static final String PROXY_PASSWORD = "s3fs_proxy_password";
	public static final String PROXY_PORT = "s3fs_proxy_port";
	public static final String PROXY_USERNAME = "s3fs_proxy_username";
	public static final String PROXY_WORKSTATION = "s3fs_proxy_workstation";
	public static final String SOCKET_SEND_BUFFER_SIZE_HINT = "s3fs_socket_send_buffer_size_hint";
	public static final String SOCKET_RECEIVE_BUFFER_SIZE_HINT = "s3fs_socket_receive_buffer_size_hint";
	public static final String SOCKET_TIMEOUT = "s3fs_socket_timeout";
	public static final String USER_AGENT = "s3fs_user_agent";

	AmazonS3 getAmazonS3(URI uri, Properties props);

}