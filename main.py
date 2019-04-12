# -*- coding: utf-8 -*-

"""
***************************************************************************
    main.py
    -------------------------
    begin                : April 2019
    author               : (C) 2019 by Luigi Pirelli
    author email         : luipir at gmail dot com
    copyright            : (C) 2019 INSITU
    company web          : https://ingenieriainsitu.com/en/
***************************************************************************
*                                                                         *
*   This program is free software; you can redistribute it and/or modify  *
*   it under the terms of the GNU General Public License as published by  *
*   the Free Software Foundation; either version 2 of the License, or     *
*   (at your option) any later version.                                   *
*                                                                         *
***************************************************************************
"""

__author__ = 'Luigi Pirelli'
__date__ = 'April 2019'
__copyright__ = '(C) 2019, INSITU'

__revision__ = '$Format:%H$'

import logging
import signal
import time
import threading
import queue

from .azure_queue_manager import AzureQueueManager

def setupLogger():
    '''set global logger instance.'''
    logger = logging.getLogger('orchestrator')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    shandler = logging.StreamHandler(stream=sys.stdout)
    shandler.setFormatter(formatter)
    logger.addHandler(handler)
    shandler = logging.FileHandler('orchestrator.log')
    shandler.setFormatter(formatter)
    logger.addHandler(handler)

def populateMessageQueue(
        queue_manager,
        topic_name,
        subscription_name,
        queue,
        wait_interval):
    '''
    Collect ServiceBus messages and put them in async 'queue'.
    '''
    while True:
        message = queue_manager.peekMessage(topic_name, 
                                            subscription_name)
        if message:
            queue.put(message)

        time.sleep(wait_interval)

def main():
    setupLogger()

    # gently manage control-c interrupt
    # if necessary
    def control_c_handler(sig, frame):
        raise Exception("Control-C received")
    signal.signal(signal.SIGINT, control_c_handler)

    # create queue connector
    service_bus = AzureQueueManager()
    service_bus.connect()
    service_bus.topics() # test connection credential asking for topics

    # start reading message queue
    queue = queue.Queue()
    queueListenerThread = Thread(target=populateMessageQueue,
                                 args=(service_bus, 'test-topic', 'test-suscriber-1', queue, 10000))
    queueListenerThread.daemon = True
    queueListenerThread.start() #start collecting message form queue

    # main loop
    while True:
        message = none
        try:
            message = queue.get(block=False, timeout=0)
        except queue.empty:
            pass

        if message:
            # parse
            # if drone start ODB
            # if satellite start GDAL

            # started have to be put in queue to check if finished with relative message

            # check queued processes
            
            # if queued process ed => unlog message and remove it
            pass

        time.sleep(2000)


def __main__():
    try:
        main()
    except:
        logger = logging.getLogger('orchestrator')
        logger.exception()