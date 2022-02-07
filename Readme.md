## PerfSONAR Collector

It consists of 9 separate collectors each subscribing to one RabbitMQ topic, parses data, and sends it to Elasticsearch.

Mandatory environment variables are:

* RMQ_VHOST
* RMQ_USER
* RMQ_PASS
* RMQ_ID

Optional environment variables are:

* HOSTCERT - path to grid certificate used to connect to GOCDB/OIM
* HOSTKEY - path to grid certificate key used to connect to GOCDB/OIM

Directory containing HOSTCERT/HOSTKEY can be attached to the docker container as volume (using -v)

Default Elasticsearch is set to atlas-kibana.mwt2.org:9200. Only firewall whitelisted clients can write into it.
To use different ES set these environment variables:

* ES_HOST
* ES_USER
* ES_PASS

## ESNET Collector
