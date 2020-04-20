
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient


class ElasticsearchClientFactory(object):
    """
    Return a CEDA elasticsearch client object
    """

    @staticmethod
    def get_client(config_args):
        """
        Return an appropriately configured Elasticsearch client.

        :param config_args: Configuration dictionary. Should contain an Elasticsearch hostname under key 'es-host' and an Elasticsearch port under the key 'es-port'.
        :returns: A configured Elasticsearch instance
        """
        # Set params
        api_key = config_args["es-configuration"]["api-key"]

        return CEDAElasticsearchClient(
            headers={
                'x-api-key': api_key
            },
            timeout=60)
