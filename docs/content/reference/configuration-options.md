# Configuration Options

## Environment Variables

A complete list of environment variables which can be set to configure the client.

| Key                                       | Default | Description                                                                                                             |
| ------------------------------------------|---------|-------------------------------------------------------------------------------------------------------------------------|
| s3fs.access.key                           | none    | <small>AWS access key, used to identify the user interacting with AWS</small>                                           |
| s3fs.secret.key                           | none    | <small>AWS secret access key, used to authenticate the user interacting with AWS</small>                                |
| s3fs.request.metric.collector.class       | TODO    | <small>Fully-qualified class name to instantiate an AWS SDK request/response metric collector</small>                   |
| s3fs.connection.timeout                   | TODO    | <small>Timeout (in milliseconds) for establishing a connection to a remote service</small>                              |
| s3fs.max.connections                      | TODO    | <small>Maximum number of connections allowed in a connection pool</small>                                               |
| s3fs.max.retry.error                      | TODO    | <small>Maximum number of times that a single request should be retried, assuming it fails for a retryable error</small> |
| s3fs.protocol                             | TODO    | <small>Protocol (HTTP or HTTPS) to use when connecting to AWS</small>                                                   |
| s3fs.proxy.domain                         | none    | <small>For NTLM proxies: The Windows domain name to use when authenticating with the proxy</small>                      |
| s3fs.proxy.host                           | none    | <small>Proxy host name either from the configured endpoint or from the "http.proxyHost" system property</small>         |
| s3fs.proxy.password                       | none    | <small>The password to use when connecting through a proxy</small>                                                      |
| s3fs.proxy.port                           | none    | <small>Proxy port either from the configured endpoint or from the "http.proxyPort" system property</small>              |
| s3fs.proxy.username                       | none    | <small>The username to use when connecting through a proxy</small>                                                      |
| s3fs.proxy.workstation                    | none    | <small>For NTLM proxies: The Windows workstation name to use when authenticating with the proxy</small>                 |
| s3fs.region                               | none    | <small>The AWS Region to configure the client</small>                                                                   |
| s3fs.socket.send.buffer.size.hint         | TODO    | <small>The size hint (in bytes) for the low level TCP send buffer</small>                                               |
| s3fs.socket.receive.buffer.size.hint      | TODO    | <small>The size hint (in bytes) for the low level TCP receive buffer</small>                                            |
| s3fs.socket.timeout                       | TODO    | <small>Timeout (in milliseconds) for each read to the underlying socket</small>                                         |
| s3fs.user.agent.prefix                    | TODO    | <small>Prefix of the user agent that is sent with each request to AWS</small>                                           |
| s3fs.amazon.s3.factory.class              | TODO    | <small>Fully-qualified class name to instantiate a S3 factory base class which creates a S3 client instance</small>     |
| s3fs.signer.override                      | TODO    | <small>Fully-qualified class name to define the signer that should be used when authenticating with AWS</small>         |
| s3fs.path.style.access                    | TODO    | <small>Boolean that indicates whether the client uses path-style access for all requests</small>                        |
| s3fs.request.header.cache-control         | blank   | <small>Configures the `cacheControl` on request builders (i.e. `CopyObjectRequest`, `PutObjectRequest`, etc)            | 
