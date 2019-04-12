# -*- coding: utf-8 -*-

"""
***************************************************************************
    queue_manager.py
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

from abc import ABC, abstractmethod


class QueueManager(ABC):
    '''Class to abstract the queue infrastructure.'''

    logger = None

    def __init__():
        self.logger = logging.getLogger('orchestrator')
        pass

    @abstractmethod
    def get_live_servicebus_config():
        '''get connection configuration
        @return: key:value dict with connection parameters'''
        return {}

    @abstractmethod
    def connect():
        '''Setup connection.'''
        pass
    
    @abstractmethod
    def isConnected():
        pass
    
    @abstractmethod
    def topics():
        pass

    @abstractmethod
    def subscribe(topic_name, subscribe_name):
        '''Subscribe to a topic: topic_name and calling 
        subscription as subscribe_name.
        '''
        pass

    @abstractmethod
    def removeSubscribtion(topic_name, subscribe_name):
        '''Remove subscription named: subscribe_name to a topic: topic_name.
        '''
        pass

    @abstractmethod
    def peekMessage(topic_name, subscription_name, timeout=60):
        '''Peek a message without removing and loking it.
        '''
        pass

    @abstractmethod
    def removeMessage(topic_name, subscription_name, identifier={}):
        '''Remove a message of topic_name in a subscription_name and identified
        by a set of parameter. Every implementer have to manage these set of parameters
        '''
        pass

    @abstractmethod
    def unlockMessage(topic_name, subscription_name, identifier={}):
        '''unlock a message of topic_name in a subscription_name and identified
        by a set of parameter. Every implementer have to manage these set of parameters
        '''
        pass
