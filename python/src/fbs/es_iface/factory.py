
from random import randint

from elasticsearch import Elasticsearch, ConnectionError


class ElasticsearchClientFactory(object):

    def get_client(self, config_args):
        """
        Return an appropriately configured Elasticsearch client.

        :param config_args: Configuration dictionary. Should contain an Elasticsearch hostname under key 'es-host' and an Elasticsearch port under the key 'es-port'.
        :returns: A configured Elasticsearch instance
        """
        hosts = config_args["es-configuration"]["es-host"]

        # Do some load balancing...
        host_list = hosts.split(",")
        number_of_hosts = len(host_list)
        host_to_use = randint(0, (number_of_hosts - 1))

        # Set params
        host = host_list[host_to_use]
        port = config_args["es-configuration"]["es-port"]
        username = config_args["es-configuration"]["es-username"]
        password = config_args["es-configuration"]["es-password"]

        return Elasticsearch(hosts=["{}:{}".format(host, port)],
                             http_auth=(username, password),
                             timeout=60)
