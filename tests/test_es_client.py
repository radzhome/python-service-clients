#!/usr/bin/env python
"""Tests search connection methods"""
import logging
import sys
import unittest

sys.path.insert(0, '..')
from service_clients.search.es_client import ESClient

ES_CONN_PARAMS = {
    'hosts': [{'host': 'localhost', 'port': 9200, 'timeout': 1}, ]
}

TEST_ES_INDEX = 'test_es_index'


class TestESClient(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up TestES')
        self.es_client = ESClient(ES_CONN_PARAMS)

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down TestES')

    def shortDescription(self):
        return None

    # Test start below

    def test_connection1(self):
        es_client = ESClient({})  # Should gt default config
        es_conn = es_client.connection
        assert es_conn

    def test_connection2(self):
        es_client = ESClient(config=None)
        es_conn = es_client.connection
        assert es_conn

    def test_connection3(self):
        # Bad config file, not a list
        config = {
            'host': 'localhost',
            'port': 9200
        }
        with self.assertRaises(TypeError):
            ESClient(config=config)

    def test_connection4(self):
        # Bad config
        config = [{
            'host': 'localhost',
            'port': 9200
        }]
        with self.assertRaises(AttributeError):
            es_client = ESClient(config=config)
            es_conn = es_client.connection
            assert es_conn

    def test_connection5(self):
        es_client = ESClient(config=ES_CONN_PARAMS)
        es_conn = es_client.connection
        assert es_conn

    def test_connection6(self):
        config = {
            'hosts': [
                {'host': 'localhost', 'port': 9200 }
            ]
        }
        es_client = ESClient(config=config)
        es_conn = es_client.connection
        assert es_conn

    def test_search1(self):
        """
        Single query search
        """
        self.es_client.create_index(TEST_ES_INDEX)
        q = """
        {
            "min_score": 2.0,
            "track_scores": true,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "venue_name": {
                                    "query": "dodger stadium",
                                    "operator": "and"
                                }
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "match": {
                                            "name": {
                                                "query": "ironman",
                                                "minimum_should_match": "33%",
                                                "fuzziness": "AUTO"
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
        """
        assert isinstance(self.es_client.search(q, index_name=TEST_ES_INDEX), list)

    def test_msearch1(self):
        """
        Test multi-search
        """
        self.es_client.create_index(TEST_ES_INDEX)

        # Multiple queries
        queries = []
        queries.append(
            {"min_score": 2.0, "query": {"bool": {"should": [{"match": {"name": {"query": "batman"}}}]}}}
        )
        queries.append(
            {"min_score": 1.0, "query": {"bool": {"should": [{"match": {"name": {"query": "ironman"}}}]}}}
        )
        queries.append(
            {"track_scores": True, "min_score": 9.0, "query":
                {"bool": {"should": [{"match": {"name": {"query": "not-findable"}}}]}}}
        )
        q_results = self.es_client.msearch(queries, index_name=TEST_ES_INDEX, doc_type='event')

        assert len(q_results) == 3

    def test_get_doc1(self):
        """
        Get a non existent doc
        :return:
        """
        doc = self.es_client.get_document(12345, index_name=TEST_ES_INDEX)
        assert not doc, "Got {}".format(doc)

    def test_upsert1(self):
        """
        Test inserting doc, then updating it
        """
        doc_id = 1
        doc_title = 'Something to upsert'
        document = {
            '_id': doc_id,  # This will be dropped anyways
            'title': doc_title,
            'date': '2017-12-12 22:00:00',
            'time': '10:00 pm',
        }
        self.es_client.create_index(TEST_ES_INDEX)

        # Insert
        updated = self.es_client.upsert_document(index_name=TEST_ES_INDEX, body=document, doc_id=doc_id)
        assert isinstance(updated, dict), type(updated)
        assert int(updated['_id']) == doc_id, "Got {}".format(doc_id)

        # Check ok
        updated = self.es_client.get_document(doc_id, index_name=TEST_ES_INDEX)
        assert updated['title'] == doc_title

        # Update
        doc_title_new = 'Some Test'
        document['title'] = doc_title_new
        update = self.es_client.upsert_document(TEST_ES_INDEX, document, doc_id=doc_id)
        assert isinstance(update, dict), type(update)
        assert int(update['_id']) == doc_id, "Got {}".format(doc_id)

        # Check ok
        updated = self.es_client.get_document(doc_id, index_name=TEST_ES_INDEX)
        assert updated['title'] == doc_title_new

    def test_bulk_update_index1(self):
        documents = [{
            '_id': 1,
            'title': 'Something to upsert',
            'date': '2017-12-12 22:00:00',
            'time': '10:00 pm',
        },
            {
                '_id': 1,
                'title': 'Something to upsert',
                'date': '2017-12-12 22:00:00',
                'time': '10:00 pm',
            },
        ]
        self.es_client.create_index(TEST_ES_INDEX)
        self.es_client.bulk_update_index(documents, index_name=TEST_ES_INDEX)

    def test_remove1(self):
        doc_id = 1
        doc = {
            '_id': doc_id,
            'name': 'Test Name',
            'date': '2017-12-12 22:00:00',
            'time': '10:00 pm',
            'title': 'Test title',
        }
        test_index_name = TEST_ES_INDEX
        self.es_client.create_index(test_index_name)
        self.es_client.upsert_document(test_index_name, doc, doc_id=doc_id)

        remove = self.es_client.remove_document(test_index_name, doc_id=doc_id)
        assert isinstance(remove, dict)
        assert int(remove['_id']) == doc_id, "Got {}".format(doc_id)

    def test_add_remove_alias1(self):
        """
        Add as a list, multi-indexes
        """
        self.es_client.create_index(TEST_ES_INDEX)
        alias = 'test_alias1'
        self.es_client.delete_alias(index_name=TEST_ES_INDEX, alias_name=alias)

        result = self.es_client.add_alias(indexes=[TEST_ES_INDEX, ], alias_name=alias)
        assert result['acknowledged']

        result = self.es_client.delete_alias(index_name=TEST_ES_INDEX, alias_name=alias)
        assert result['acknowledged']

    def test_add_remove_alias2(self):
        """
        Add as an alias for single index
        """
        alias = 'test_alias1'
        self.es_client.delete_alias(index_name=TEST_ES_INDEX, alias_name=alias)

        result = self.es_client.add_alias(indexes=TEST_ES_INDEX, alias_name=alias)
        assert result['acknowledged']

        result = self.es_client.delete_alias(index_name=TEST_ES_INDEX, alias_name=alias)
        assert result['acknowledged']

    def test_create_delete_index1(self):
        assert isinstance(self.es_client.create_index(TEST_ES_INDEX), bool)  # Could be created, or already exists
        result = self.es_client.delete_index(TEST_ES_INDEX)
        assert result['acknowledged']

    def test_setup_index1(self):
        index = 'test_es_setup_index'
        self.es_client.delete_index(index)
        self.es_client.create_index(index)
        settings = {
            'settings': {
                'index': {
                    'number_of_replicas':  0
                }
            }
        }
        mapping = {
            '_doc': {
                # 'dynamic': "false",
                'properties': {
                    'advertorial': {
                        'type': "boolean"
                    },
                    'client_id': {
                        'type': "keyword"
                    },
                }
            }
        }

        mappings = {
            "_doc": {
                "dynamic": "false",
                "properties": {
                    "field1": {"type": "keyword"}
                }
            }
        }

        self.es_client.create_index(index, replace=True)
        assert self.es_client.setup_index(index, mappings, settings) == {u'acknowledged': True}


    def test_create_index1(self):
        index_name = 'test_es_setup_index'
        success = self.es_client.create_index(index_name)
        assert isinstance(success, bool), "Got {}".format(type(success))

    def test_create_index2(self):
        index_name = 'test_es_setup_index'
        assert self.es_client.create_index(index_name, replace=True)

    def test_get_alias1(self):
        index_name = 'test_es_setup_index'
        assert not self.es_client.get_alias(index_name=index_name)[index_name]['aliases']

    def test_get_alias2(self):
        alias_name = 'test_es_setup_non_existent'
        assert not self.es_client.get_alias(alias_name=alias_name)


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
