# import threading
import time

import greenlet
from twisted.internet import defer, threads
from twisted.internet.error import ReactorAlreadyInstalledError
from twisted.internet.task import deferLater
from twisted.python import failure


class TwistedObjects:
    _REACTOR = None
    _GREENLET = None
    _TWISTED_THREAD = None


def deferred_sleep(secs):
    return deferLater(TwistedObjects._REACTOR, secs, lambda: None)


def sleep_in_reactor_thread(secs):
    threads.blockingCallFromThread(TwistedObjects._REACTOR, time.sleep, secs)


def pytest_configure():
    init_reactor1()


def run_in_reactor_thread(fn, *args, **kwargs):
    result = threads.blockingCallFromThread(TwistedObjects._REACTOR, fn, *args, **kwargs)
    return result


def wait_for_deferred(d):
    if not isinstance(d, defer.Deferred):
        raise TypeError("Cannot wait for a non-Deferred type.")

    main = greenlet.getcurrent()
    if main is TwistedObjects._GREENLET:
        raise RuntimeError("Main greenlet should not be the twisted greenlet")

    result = []

    def callback(res):
        result.append(res)
        # move the result back to the main greenlet
        if greenlet.getcurrent() is not main:
            main.switch(result)

    d.addCallbacks(callback, callback)
    if not result:
        # switch to twisted greenlet and keep context when switched back
        twisted_res = TwistedObjects._GREENLET.switch()
        if twisted_res is not result:
            raise RuntimeError("Results should match after greenlet switch")

    final_result = result[0]
    if isinstance(final_result, failure.Failure):
        final_result.raiseException()

    return final_result


def kill_twisted_greenlet(d):
    main = greenlet.getcurrent()
    result = []

    def callback(res):
        result.append(res)
        # move the result back to the main greenlet
        if greenlet.getcurrent() is not main:
            main.switch(result)

    # we handle the result/exception later, so the same callback can be used for
    # callback and errback
    d.addCallbacks(callback, callback)
    if not result:
        # switch to twisted greenlet and keep context when switched back
        twisted_res = TwistedObjects._GREENLET.switch()
        if twisted_res is not result:
            raise RuntimeError(
                "Result instance returned from twisted greenlet doesn't match main greenlet result instance.")

    final_result = result[0]
    if isinstance(final_result, failure.Failure):
        final_result.raiseException()

    return "twisted greenlet dead"


def init_reactor():
    import asyncio

    from twisted.internet import asyncioreactor
    try:
        asyncioreactor.install(asyncio.get_event_loop())
    except ReactorAlreadyInstalledError as ex:
        print(ex)
    finally:
        import twisted.internet.reactor

    TwistedObjects._REACTOR = twisted.internet.reactor
    if not hasattr(TwistedObjects._REACTOR, '_asyncioEventloop'):
        raise RuntimeError(
            "Reactor installed is not the asyncioreactor.")

    TwistedObjects._GREENLET = greenlet.greenlet(TwistedObjects._REACTOR.run)
    failure.Failure.cleanFailure = lambda self: None


def run_reactor(reactor):
    reactor.run()


def init_reactor1():
    import asyncio

    from twisted.internet import asyncioreactor
    try:
        asyncioreactor.install(asyncio.get_event_loop())
    except ReactorAlreadyInstalledError as ex:
        print(ex)
    finally:
        import twisted.internet.reactor

    TwistedObjects._REACTOR = twisted.internet.reactor
    if not hasattr(TwistedObjects._REACTOR, '_asyncioEventloop'):
        raise RuntimeError(
            "Reactor installed is not the asyncioreactor.")

    # TwistedObjects._TWISTED_THREAD = threading.Thread(target=lambda: run_reactor(TwistedObjects._REACTOR))
    # TwistedObjects._REACTOR.suggestThreadPoolSize(10)
    # TwistedObjects._TWISTED_THREAD.start()

# hook to catch prior to running tests
# def pytest_runtest_call(item):
#   pass


# def pytest_unconfigure():

#     # import time
#     # time.sleep(1)

#     # # if the reactor is running, stop it and kill the twisted greenlet
#     # if TwistedObjects._REACTOR.running:
#     #     TwistedObjects._REACTOR.stop()
#     #     TwistedObjects._GREENLET.switch()

#     # # if the twisted greenlet isn't dead, make it dead
#     # if not TwistedObjects._GREENLET.dead:
#     #     d = defer.Deferred()
#     #     # just a fake deferred
#     #     TwistedObjects._REACTOR.callLater(0.0, d.callback, "make dead")
#     #     kill_twisted_greenlet(d)

#     threads.blockingCallFromThread(TwistedObjects._REACTOR, TwistedObjects._REACTOR.stop)
#     TwistedObjects._TWISTED_THREAD.join()
