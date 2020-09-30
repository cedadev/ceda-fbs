# Guide to This Directory

* "mapping" - Index mapping for ElasticSearch, along with JSON schema for metadata
* "sample-queries" - Sample queries for ElasticSearch installations containing the metadata conforming to the schema
    * Submit with ```curl -XPOST -d @sample_query_filename``` (the @ symbol tells CURL to get data from a file)
