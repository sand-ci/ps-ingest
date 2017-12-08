curl -XPOST 'http://atlas-kibana.mwt2.org:9200/_template/perfsonar' -d '{
  "template" : "network_weather-*",
  "mappings": {
      "throughput": {
        "properties": {
          "MA": {"type": "keyword" },
          "src": {"type": "keyword" },
          "srcSite": {"type": "keyword" },
          "srcVO": {"type": "keyword" }, 
          "srcProduction": {"type": "boolean"},
          "dest":  {"type": "keyword" },
          "destSite": {"type": "keyword" },
          "destVO": {"type": "keyword" },
          "destProduction": { "type": "boolean"},
	      "timestamp": { "type": "date", "format": "basic_date_time_no_millis||epoch_millis"},
          "throughput": { "type": "long" }
        }
      },
      "packet_loss_rate": {
        "properties": {
          "MA": {"type": "keyword" },
          "src": {"type": "keyword" },
          "srcSite": {"type": "keyword" },
          "srcVO": {"type": "keyword" }, 
          "srcProduction": {"type": "boolean"},
          "dest":  {"type": "keyword" },
          "destSite": {"type": "keyword" },
          "destVO": {"type": "keyword" },
          "destProduction": { "type": "boolean"},
          "packet_loss": { "type": "float" },
          "timestamp": { "type": "date", "format": "basic_date_time_no_millis||epoch_millis"}
        }
      },
      "traceroute": {
        "properties": {
          "MA": {"type": "keyword" },
          "dest": {"type": "keyword" },
          "destProduction": { "type": "boolean"},
          "destSite": {"type": "keyword" },
          "destVO": {"type": "keyword" },
          "src": {"type": "keyword" },
          "srcProduction": {"type": "boolean"},
          "srcSite": {"type": "keyword" },
          "srcVO": {"type": "keyword" },
          "timestamp": { "type": "date",  "format": "basic_date_time_no_millis||epoch_millis"},
          "hops":{"type": "keyword" },
	      "ttls":{"type":"integer"},
	      "rtts":{"type":"float"},
	      "hash":{"type":"long"}
        }
      },
      "latency": {
        "properties": {
          "MA": {"type": "keyword" },
          "dest":  {"type": "keyword" },
          "destProduction": { "type": "boolean"},
          "destSite": {"type": "keyword" },
          "destVO": {"type": "keyword" },
          "src": {"type": "keyword" },
          "srcProduction": {"type": "boolean"},
          "srcSite": {"type": "keyword" },
          "srcVO": {"type": "keyword" },
          "timestamp": {"type": "date", "format": "basic_date_time_no_millis||epoch_millis" },
          "delay_mean": {"type": "float"},
          "delay_median": {"type": "float"},
          "delay_sd": {"type": "float"}
        }
      },
      "netflow_lhcopn": {
        "properties": {
          "MA": {"type": "keyword" },
          "dstInterface": {"type": "keyword" },
          "destVO": {"type": "keyword" },
          "srcInterface": {"type": "keyword" },
          "srcVO": {"type": "keyword" },
          "timestamp": {"type": "date", "format": "basic_date_time_no_millis||epoch_millis" },
          "utilization": {"type": "float"}
        }
      },
      "link_utilization": {
        "properties": {
          "MA": {"type": "keyword" },
          "dest":  {"type": "keyword" },
          "destProduction": { "type": "boolean"},
          "destSite": {"type": "keyword" },
          "destVO": {"type": "keyword" },
          "src": {"type": "keyword" },
          "srcProduction": {"type": "boolean"},
          "srcSite": {"type": "keyword" },
          "srcVO": {"type": "keyword" },
          "timestamp": {"type": "date", "format": "basic_date_time_no_millis||epoch_millis" },
          "sim_util": {"type": "float"}
        }
      },
      "retransmits": {
        "properties": {
          "MA": {"type": "keyword" },
          "dest":  {"type": "keyword" },
          "destProduction": { "type": "boolean"},
          "destSite": {"type": "keyword" },
          "destVO": {"type": "keyword" },
          "src": {"type": "keyword" },
          "srcProduction": {"type": "boolean"},
          "srcSite": {"type": "keyword" },
          "srcVO": {"type": "keyword" },
          "timestamp": {"type": "date", "format": "basic_date_time_no_millis||epoch_millis" },
          "retransmits": {"type": "float"}
        }
      },
      "meta": {
        "properties": {
          "administrator": {
            "properties": {
              "email":{"type": "keyword" },
              "name":{"type": "keyword" },
              "organization": {"type": "keyword" }
            }
          },
          "communities": {"type": "keyword" },
          "distribution": {"type": "keyword" },
          "external_address": {
            "properties": {
              "address":{"type": "keyword" },
              "dns_name": {"type": "keyword" },
              "iface": {"type": "keyword" },
              "ipv4_address": {"type": "keyword" },
              "ipv6_address": {"type": "keyword" }
            }
          },
          "geolocation": {"type": "geo_point"},
          "ls_client_uuid": {  "type": "text"  },
          "meshes": {   "type": "text"  },
          "ntp": {
            "properties": {
              "address": {"type": "keyword" },
              "host": {"type": "keyword" }
            }
          },
          "product_name": {"type": "keyword" },
          "services": {
            "properties": {
              "bwctl": {
                "properties": {
                  "addresses":{"type": "keyword" },
                  "daemon_port": {"type": "keyword" },
                  "enabled": {"type": "keyword" },
                  "is_running": {"type": "keyword" },
                  "version": {"type": "keyword" }               
	            }
              },
              "esmond": {
                "properties": {
                  "addresses":{"type": "keyword" },
                  "enabled": {"type": "keyword" },
                  "is_running":{"type": "keyword" },
                  "version": {"type": "keyword" }
                }
              },
              "iperf3": {
                "properties": {
                  "is_running":{"type": "keyword" },
                  "version":{"type": "keyword" }
                }
              },
              "lsregistration": {
                "properties": {
                  "enabled": {"type": "keyword" },
                  "is_running":{"type": "keyword" },
                  "version": {"type": "keyword" }
                }
              },
              "meshconfig-agent": {
                "properties": {
                  "enabled":{"type": "keyword" },
                  "is_running":{"type": "keyword" },
                  "version": {"type": "keyword" }
                }
              },
              "ndt": {
                "properties": {
                  "addresses": {"type": "keyword" },
                  "daemon_port": {"type": "keyword" },
                  "is_running": {"type": "keyword" },
                  "version":{"type": "keyword" }
                }
              },
              "npad": {
                "properties": {
                  "addresses": {"type": "keyword" },
                  "daemon_port":{"type": "keyword" },
                  "is_running": {"type": "keyword" },
                  "version":{"type": "keyword" }
                }
              },
              "owamp": {
                "properties": {
                  "addresses": {"type": "keyword" },
                  "daemon_port":{"type": "keyword" },
                  "enabled": {"type": "keyword" },
                  "is_running":{"type": "keyword" },
                  "version": {"type": "keyword" }
                }
              },
              "pscheduler": {
                "properties": {
                  "addresses":{"type": "keyword" },
                  "enabled": {"type": "keyword" },
                  "is_running": {"type": "keyword" },
                  "version": {"type": "keyword" }
                }
              },
              "regular_testing": {
                "properties": {
                  "enabled": {"type": "keyword" },
                  "is_running":{"type": "keyword" },
                  "version": {"type": "keyword" }
                }
              }
            }
          },
          "sys_vendor": {"type": "keyword" },
          "timestamp": {
            "type": "date",
            "format": "basic_date_time_no_millis||epoch_millis"
          },
          "toolkit_name": {"type": "keyword" },
          "toolkit_rpm_version": {"type": "keyword" },
          "toolkit_version": {"type": "keyword" }
        }
      }
    }
}'
