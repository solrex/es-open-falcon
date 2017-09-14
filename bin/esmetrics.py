#!/usr/bin/env python

import threading
import elasticsearch
import json
import time
import requests

class EsMetrics(threading.Thread):
    status_map = {
        'green': 0,
        'yellow': 1,
        'red': 2
    }

    def __init__(self, falcon_url, es_url, es_endpoint, falcon_step = 60, daemon = False):
        self.falcon_url = falcon_url
        self.falcon_step = falcon_step
        self.es_url = es_url
        self.es_endpoint = es_endpoint
        self.index_metrics = {
            'search': ['query_total', 'query_time_in_millis', 'query_current', 'fetch_total', 'fetch_time_in_millis', 'fetch_current'],
            'indexing': ['index_total', 'index_current', 'index_time_in_millis', 'delete_total', 'delete_current', 'delete_time_in_millis'],
            'docs': ['count', 'deleted'],
            'store': ['size_in_bytes', 'throttle_time_in_millis']
        }
        self.cluster_metrics = ['status', 'number_of_nodes', 'number_of_data_nodes', 'active_primary_shards', 'active_shards', 'unassigned_shards']
        self.counter_keywords = ['query_total', 'query_time_in_millis',
            'fetch_total', 'fetch_time_in_millis',
            'index_total', 'index_time_in_millis', 
            'delete_total', 'delete_time_in_millis']
        super(EsMetrics, self).__init__(None, name=es_endpoint)
        self.setDaemon(daemon)

    def run(self):
        try:
            self.es = elasticsearch.Elasticsearch([self.es_url])
        except Exception, e:
            print "ERROR: [%s]" % self.es_endpoint, e
            return
        falcon_metrics = []
        # Statistics
        try:
            timestamp = int(time.time())
            nodes_stats = self.es.nodes.stats()
            cluster_health = self.es.cluster.health()
            keyword_metric = {}
            for node in nodes_stats['nodes']:
                index_stats = nodes_stats['nodes'][node]['indices']
                for type in self.index_metrics:
                    for keyword in self.index_metrics[type]:
                        if keyword not in keyword_metric:
                            keyword_metric[keyword] = 0
                        keyword_metric[keyword] += index_stats[type][keyword]
            for keyword in self.cluster_metrics:
                if keyword == 'status':
                    keyword_metric[keyword] = self.status_map[cluster_health[keyword]]
                else:
                    keyword_metric[keyword] = cluster_health[keyword]
            for keyword in keyword_metric:
                falcon_metric = {
                    'counterType': 'COUNTER' if keyword in self.counter_keywords else 'GAUGE',
                    'metric': "es." + keyword,
                    'endpoint': self.es_endpoint,
                    'timestamp': timestamp,
                    'step': self.falcon_step,
                    'tags': 'n=' + nodes_stats['cluster_name'],
                    'value': keyword_metric[keyword]
                }
                falcon_metrics.append(falcon_metric)
            #print json.dumps(falcon_metrics)
            req = requests.post(self.falcon_url, data=json.dumps(falcon_metrics))
            print "INFO: [%s]" % self.es_endpoint, "[%s]" % self.falcon_url, req.text
        except Exception, e:
            print "ERROR: [%s]" % self.es_endpoint, e
            return

if __name__ == '__main__':
    EsMetrics('', 'http://127.0.0.1:9200', '').run()

