import logging

def logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter("[%(asctime)s] - [%(module)s] [%(thread)d] - %(levelname)s - %(message)s")
    # add formatter to ch
    consoleHandler.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(consoleHandler)
    return logger