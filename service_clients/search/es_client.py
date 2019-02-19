"""
ElasticSearch client interface
"""
import logging
import time
import json
import copy

from elasticsearch import helpers
from elasticsearch import exceptions as es_exceptions
from elasticsearch import Elasticsearch

# Defaults
BULK_SIZE = 10000
ES_ID_FIELD = '_id'
RECONNECT_SLEEP_SEC = 1  # Timeout between re-connect attempts
REQUEST_TIMEOUT_SEC = 20  # Default is 10s. The last major TransportError lasted 40s
RETRY_ATTEMPTS = 60
RETRY_ON_TIMEOUT = True


class ESClient:

    def __init__(self, config):
        """
        Init & Configure ES connection
        :param config: dict, config
        """
        self.config = config or {}
        if self.config:
            self.hosts = [{'host': h['host'], 'port': h['port']} for h in self.config.get('hosts')]
        else:
            self.hosts = [{'host': 'localhost'}]

        self.bulk_size = self.config.get("bulk_size") or BULK_SIZE
        self.connection = None
        self.connect()

    def connect(self):
        """
        Establish ES connection
        """
        try:
            self.connection = Elasticsearch(self.hosts, retry_on_timeout=RETRY_ON_TIMEOUT,
                                            timeout=REQUEST_TIMEOUT_SEC)
        except Exception as e:  # pragma: no cover
            logging.error("ESClient.connect failed with params {}, error {}".format(self.hosts, e))

    def count_index(self, index):
        """
        Count docs in index
        :param index:
        :return:
        """
        return self.connection.count(index).get('count')

    def search(self, query=None, index_name=None, retries=0, query_type='search'):
        """
        ES search query
        :param query: dict, es query
        :param index_name: str, index to query against
        :param retries: int, current retry attempt
        :param query_type: str, search or aggregation
        :return: list, found docs
        """
        resp = []
        try:
            resp = self.connection.search(body=query, index=index_name)
            if query_type == 'search':
                found = resp['hits']['hits']
            else:  # elif query_type == 'aggregation':
                found = resp['aggregations']
        except KeyError:  # No hits key in response
            logging.critical("ESClient.search invalid response {}".format(resp))
            if retries > RETRY_ATTEMPTS:  # pragma: no cover
                logging.error("ESClient.search max attempts exceeded (key error)")
                raise
            found = self.search(query=query, index_name=index_name, retries=retries + 1)
        except es_exceptions.RequestError as e:  # pragma: no cover
            logging.critical("ESClient.search error {} on query {}".format(e, query))
            raise
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError,
                es_exceptions.TransportError):  # pragma: no cover
            logging.warning("ESClient.search connection failed, retrying...")  # Retry on timeout
            if retries > RETRY_ATTEMPTS:  # pragma: no cover
                logging.error("ESClient.search max attempts exceeded")
                raise
            time.sleep(RECONNECT_SLEEP_SEC)
            self.connect()  # Not sure if this is helpful
            found = self.search(query=query, index_name=index_name, retries=retries + 1)
        except Exception as e:  # pragma: no cover
            logging.critical("ESClient.search error {} on query {}".format(e, query))
            raise

        return found

    def msearch(self, queries, index_name, doc_type='_doc', retries=0, chunk_size=100):
        """
        Es multi-search query
        :param queries: list of dict, es queries
        :param index_name: str, index to query against
        :param doc_type: str, defined doc type i.e. _doc
        :param retries: int, current retry attempt
        :param chunk_size: int, how many queries to send to es at a time
            Increase the search queue size before sending too many requests
            I.e. threadpool.search.queue_size: 50000  in es config
        :return: dict, found doc status
        """
        search_header = json.dumps({'index': index_name, 'type': doc_type})

        def chunk_queries():
            for i in range(0, len(queries), chunk_size):
                yield queries[i:i + chunk_size]

        chunked_queries = chunk_queries()

        found = []
        for query_chunk in chunked_queries:
            request = ''
            for q in query_chunk:
                # request head, body pairs
                request += '{}\n{}\n'.format(search_header, json.dumps(q))

            resp = {}
            try:
                resp = self.connection.msearch(body=request, index=index_name)
                found.extend([r['hits']['hits'] for r in resp['responses']])
            except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError,
                    es_exceptions.TransportError, KeyError) as e:  # pragma: no cover
                if retries > RETRY_ATTEMPTS:  # pragma: no cover
                    logging.error("ESClient.msearch max attempts exceeded, error {}".format(e))
                    raise

                logging.warning("ESClient.msearch connection failed, retrying...")  # Retry on timeout

                # No hits key in response, don't retry if es_rejected_execution_exception
                if e.__class__ == KeyError:
                    # 'hits' missing, could be es_rejected_execution_exception, queue capacity reached
                    logging.critical("ESClient.msearch invalid response {}".format(resp.get('responses')))
                    # if 'search_phase_execution_exception' not in str(resp):  # reason 'all shards failed'
                    if 'es_rejected_execution_exception':
                        # raise if underlying error is ConnectionRefusedError in urllib3 caused by NewConnectionError
                        logging.error("ESClient.msearch query rejected, error {}".format(e))
                        raise

                time.sleep(RECONNECT_SLEEP_SEC)
                self.connect()  # Not sure if useful
                found = self.msearch(queries=queries, index_name=index_name, retries=retries + 1)
            except Exception as e:  # pragma: no cover
                logging.critical("ESClient.msearch error {} on query {}".format(e, queries))
                raise

        return found

    def get_document(self, doc_id, index_name, doc_type='_doc', retries=0):
        """
        Get contents of a document by id
        :param doc_id: int, the document id
        :param index_name: str, document name
        :param doc_type: str, document type
        :param retries: int, current retry attempt
        :return: dict, resulting document
        """
        try:
            result = self.connection.get_source(id=doc_id, doc_type=doc_type, index=index_name, _source=True)
        except es_exceptions.NotFoundError:
            result = None
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.get_document connection failed, retrying...")  # Retry on timeout
            if retries > RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(RECONNECT_SLEEP_SEC)
            result = self.get_document(doc_id, index_name, doc_type, retries=retries + 1)

        return result

    def upsert_document(self, index_name, body, doc_id, doc_type='_doc', retries=0):
        """
        Adds new or updates existing doc
        Upsert a document into an es index
        :param index_name: str, index name
        :param body: dict, doc
        :param doc_type: str, i.e. _doc
        :param doc_id: int, document id
        :param retries: int, number of retries of the function
        :return: dict, result
        """
        # To avoid Field [_id] is a metadata field and cannot be added inside a document.
        # Use the index API request parameters.
        if ES_ID_FIELD in body:
            body = copy.deepcopy(body)
            del body[ES_ID_FIELD]  # reserved field

        try:
            result = self.connection.update(index=index_name,
                                            doc_type=doc_type,
                                            id=doc_id,
                                            body={'doc': body,
                                                  'doc_as_upsert': True})
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.upsert_document connection failed, retrying...")  # Retry on timeout
            if retries > RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(RECONNECT_SLEEP_SEC)
            result = self.upsert_document(doc_id, body, index_name, doc_type, retries=retries + 1)

        return result

    def remove_document(self, index_name, doc_id, doc_type='_doc', retries=0):
        """
        Remove a document from es index
        :param index_name: str, index name
        :param doc_id: int, doc id
        :param doc_type: str, i.e. _doc
        :param retries: int, number of retries of the function
        :return: dict, result
        """
        try:
            result = self.connection.delete(index=index_name, doc_type=doc_type, id=doc_id)
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.remove_document connection failed, retrying...")  # Retry on timeout
            if retries > RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(RECONNECT_SLEEP_SEC)
            result = self.remove_document(index_name, doc_id, doc_type, retries=retries + 1)

        return result

    def bulk_update_index(self, documents, index_name, doc_type='_doc', id_field='_id'):
        """
        Bulk populates an es index with doc data
        Can also be used to add a single doc to index
        :param index_name: str, index name
        :param documents: list, of dicts index docs
        :param doc_type: str, document type for es
        :param id_field: str, document id field name
        :return: bool, success
        """
        copied_docs = copy.deepcopy(documents)

        bulk_data = []
        for body in copied_docs:
            doc_id = body[id_field]
            if ES_ID_FIELD in body:
                del body[ES_ID_FIELD]
            bulk_data.append(
                {
                    '_index': index_name,
                    '_type': doc_type,
                    '_source': json.dumps(body),
                    '_id': doc_id
                }
            )

        success = False
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                helpers.bulk(self.connection, actions=bulk_data)
                self.connection.indices.refresh(index=index_name)
                success = True
                break
            except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
                logging.warning("ESClient.bulk_update_index connection timeout")  # Retry on timeout
                self.connect()  # Not sure if this is helpful, or connection is lazy?
                continue

        return success

    def create_index(self, index_name, body=None, replace=False):
        """
        Create an index by name, populate with body
        :param index_name: str, name of index
        :param body: dict, optional document to create
        :param replace: bool, force replace existing index
        :return: dict, created status info
        """
        result = None
        for _attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                result = self.connection.indices.create(index=index_name, ignore=400, body=body)
                result = bool('acknowledged' in result)
                break
            except es_exceptions.AuthorizationException:  # pragma: no cover
                result = False
                break
            except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError) as e:  # pragma: no cover
                logging.error("ESClient.create_index connection error: {}".format(e))  # Retry on timeout
                time.sleep(RECONNECT_SLEEP_SEC)
                self.connect()  # Not sure if this is helpful, or connection is lazy?
                continue

        if replace and not result:
            logging.warning("ESClient.create_index replacing existing index {}".format(index_name))
            self.delete_index(index_name)
            result = self.connection.indices.create(index=index_name, ignore=400, body=body)

        if result:
            self.connection.indices.refresh(index_name)

        return result

    def setup_index(self, index_name, doc_mapping, index_settings):
        """
        Set up Index
        :param index_name: str, index name
        :param index_settings: str or dict, index settings document
        :param doc_mapping: str or dict, index doc mapping schema
        :return: bool, setup settings and index success
        """
        # index_settings = index_settings or self.INDEX_SETTINGS
        doc_type = list(doc_mapping.keys())[0]
        settings = mapped = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                # close index to modify settings
                self.connection.indices.close(index=index_name)
                # Creates es analyzer, filter settings
                settings = self.connection.indices.put_settings(index=index_name, body=index_settings)
                self.connection.indices.open(index=index_name)

                # Sets up document structure / mapping
                mapped = self.connection.indices.put_mapping(index=index_name, doc_type=doc_type, body=doc_mapping)
                break
            except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
                logging.warning("ESClient.setup_index connection timeout")  # Retry on timeout
                self.connect()  # Not sure if this is helpful, or connection is lazy?
                continue

        return settings and mapped

    def delete_index(self, index_name):
        """
        Delete an index by name
        :param index_name: str, index name
        :return: dict, removed status
        """
        result = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                result = self.connection.indices.delete(index=index_name)
                break
            except es_exceptions.NotFoundError:  # pragma: no cover
                result = False
                break
            except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
                logging.warning("ESClient.delete_index connection timeout")  # Retry on timeout
                self.connect()  # Not sure if this is helpful, or connection is lazy?
                continue

        if not result:  # pragma: no cover
            logging.warning("ESClient.delete_index failed for {}".format(index_name))
        return result

    def add_alias(self, indexes, alias_name, retries=0):
        """
        Set the alias current for new index
        Note: It is possible to have one alias for multiple
        indexes but bulk populate will fail for that alias
        :param indexes: list (or single str) of index names
        :param alias_name: str, alias to use for the index
        :param retries: int, number of retries of the function
        :return: dict, added info
        """
        try:
            added = self.connection.indices.put_alias(index=indexes, name=alias_name)
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.add_alias connection failed, retrying...")  # Retry on timeout
            if retries > RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(RECONNECT_SLEEP_SEC)
            added = self.get_alias(indexes, alias_name, retries=retries + 1)
        return added

    def get_alias(self, alias_name=None, index_name=None, retries=0):
        """
        Return alias information i.e indexes either by
        alias name or index to get aliases for an index
        :param alias_name: str, alias to use for the index
        :param index_name: str, name of index
        :param retries: int, number of retries of the function
        :return:
        """
        try:
            alias = self.connection.indices.get_alias(name=alias_name, index=index_name)
        except es_exceptions.NotFoundError:  # pragma: no cover
            alias = None
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.get_alias connection failed, retrying...")  # Retry on timeout
            if retries > RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(RECONNECT_SLEEP_SEC)
            alias = self.get_alias(alias_name, index_name, retries=retries + 1)
        return alias

    def delete_alias(self, index_name, alias_name, retries=0):
        """
        Remove alias
        :param index_name: str, index name
        :param alias_name: str, alias to use for the index
        :param retries: int, number of retries of the function
        :return: dict, removed status
        """
        try:
            removed = self.connection.indices.delete_alias(name=alias_name, index=index_name)
        except es_exceptions.NotFoundError:  # pragma: no cover
            return False
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.delete_alias connection failed, retrying...")  # Retry on timeout
            if retries > RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(RECONNECT_SLEEP_SEC)
            removed = self.delete_alias(index_name, alias_name, retries=retries + 1)
        return removed
