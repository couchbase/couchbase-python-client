from twisted.internet import asyncioreactor

from acouchbase import get_event_loop

asyncioreactor.install(get_event_loop())
