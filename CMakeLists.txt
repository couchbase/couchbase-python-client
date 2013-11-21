
INSTALL(FILES
	uuid.py
        DESTINATION lib/python)

INSTALL(FILES
        couchbase/__init__.py
        couchbase/client.py
        couchbase/couchbaseclient.py
        couchbase/exception.py
        couchbase/logger.py
        couchbase/rest_client.py
        couchbase/util.py
        DESTINATION lib/python/couchbase)

INSTALL(FILES
        couchbase/migrator/__init__.py
        couchbase/migrator/migrator.py
        couchbase/migrator/migrator_couchbase.py
        couchbase/migrator/migrator_couchdb.py
        couchbase/migrator/migrator_csv.py
        couchbase/migrator/migrator_dir.py
        couchbase/migrator/migrator_json.py
        couchbase/migrator/migrator_zip.py
        DESTINATION lib/python/couchbase/migrator)

INSTALL(FILES
        couchbase/tests/__init__.py
        couchbase/tests/test_client.py
        couchbase/tests/test_couchbaseclient.py
        couchbase/tests/test_restclient.py
        couchbase/tests/warnings_catcher.py
        DESTINATION lib/python/couchbase/tests)


INSTALL(FILES
        couchbase/utils/__init__.py
        couchbase/utils/couchbase-migrator.py
        DESTINATION lib/python/couchbase/utils)

INSTALL(FILES
        httplib2/__init__.py
        httplib2/iri2uri.py
        DESTINATION lib/python/httplib2)


INSTALL(FILES
        simplejson/__init__.py
        simplejson/decoder.py
        simplejson/encoder.py
        simplejson/LICENSE.txt
        simplejson/scanner.py
        DESTINATION lib/python/simplejson)
