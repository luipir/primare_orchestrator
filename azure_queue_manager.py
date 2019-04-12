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

import logging
import ConfigParser
# manage azure.servicebus versions
try:
    # version > 0.21.1 as in m$ documentation
    # https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-python-how-to-use-topics-subscriptions
    from azure.servicebus.control_client import ServiceBusService, Message, Topic, Rule, DEFAULT_RULE_NAME
except ModuleNotFoundError:
    # version <= 0.21.1
    # as distributed as packaged module (e.g. ubuntu 18.04)
    from azure.servicebus import ServiceBusService, Message, Topic, Rule, DEFAULT_RULE_NAME

from queue_manager import QueueManager

class AzureQueueManager(QueueManager):
    '''Class to abstract the queue infrastructure
    '''

    bus_service = None

    def __init__():
        # setop logger in upper class
        super().__init__()

    def get_live_servicebus_config():
        '''inspired by https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/conftest.py'''
        parser = ConfigParser.RawConfigParser()
        parser.read('queue_account.conf')
        conf['service_bus_hostname'] = parser.get('ServiceBusService_account', 'service_bus_hostname', fallback=os.environ['SERVICE_BUS_HOSTNAME'] )
        conf['service_namespace'] = parser.get('ServiceBusService_account', 'service_namespace', fallback=os.environ['SERVICE_BUS_HOSTNAME'] )
        conf['shared_access_key_name'] = parser.get('ServiceBusService_account', 'shared_access_key_name', fallback=os.environ['SERVICE_BUS_SAS_POLICY'] )
        conf['shared_access_key_value'] = parser.get('ServiceBusService_account', 'shared_access_key_value', fallback=os.environ['SERVICE_BUS_SAS_KEY'] )
        conf['shared_access_connection_str'] = parser.get('ServiceBusService_account', 'shared_access_connection_str', fallback=os.environ['SERVICE_BUS_CONNECTION_STR'] )
        return conf

    def connect():
        '''
        Setup connection to ServiceBus.
        Set connection in file:
           ./queue_account.conf
        or settign the following env vars:
            SERVICE_BUS_HOSTNAME  # e.g. primare-desa-service-bus
            SERVICE_BUS_SAS_POLICY # eg. RootManageSharedAccessKey
            SERVICE_BUS_SAS_KEY # e.g. the key available in azure portal under RootManageSharedAccessKey
        '''
        conf = self.get_live_servicebus_config()
        try:
            if self.bus_service:
                self.logger.warning("ServiceBusService reset to new credentials")

            self.bus_service = ServiceBusService(
                service_namespace=conf['service_namespace'],
                shared_access_key_name=conf['shared_access_key_name'],
                shared_access_key_value=conf['shared_access_key_value'])

        except ValueError:
            self.bus_service = None
            self.logger.exception("Exception occurred")
    
    def isConnected():
        return self.bus_service is not None
    
    def topics():
        return self.bus_service.list_topics()

    def subscribe(topic_name, subscribe_name):
        '''Subscribe to a topic: topic_name and calling 
        subscription as subscribe_name.
        Subscription have to explicitaly removed to disconnect to  a topic.
        I't configured to avoid throwing exception if topic already exists.
        TODO: SQL filtering
        '''
        if not topic_name:
            raise Exception('No topic name set')
        if not subscribe_name:
            raise Exception('No named subscription is set')
        fail_on_exist = False
        self.bus_service.create_subscription(
            topic_name,
            subscribe_name,
            fail_on_exist
        )
    
    def removeSubscribtion(topic_name, subscribe_name):
        '''Remove subscription named: subscribe_name to a topic: topic_name.
        It's configured to avoid throwing exception if topic does not exists.
        TODO: SQL filtering
        '''
        if not topic_name:
            raise Exception('No topic name set')
        if not subscribe_name:
            raise Exception('No named subscription is set')
        fail_not_exist = False
        self.bus_service.delete_subscription(
            topic_name,
            subscribe_name,
            fail_not_exist
        )
    
    def peekMessage(topic_name, subscription_name, timeout=60):
        '''Peek a message without removing and loking it.
        Get message from topic: topic_name and subscription as subscribe_name.
        Subscription have to explicitaly removed to disconnect to  a topic.
        Unlock is done following peek_lock_subscription_message logic.
        '''
        return self.bus_service.peek_lock_subscription_message(topic_name,
                                                               subscription_name,
                                                               timeout)

    def removeMessage(
        topic_name,
        subscription_name,
        identifier={'sequence_number':None,
                    'lock_token':None}):
        '''Remove a locked message of a topic_name beloging to a subscription_name.
        Mesage is referred via sequence_number and/or lock_token retrieved
        during peekMessage/peek_lock_subscription_message
        '''
        self.bus_service.delete_subscription_message(
            topic_name=topic_name,
            subscription_name=subscription_name,
            sequence_number = identifier['sequence_number'],
            lock_token = identifier['lock_token']
        )

    def unlockMessage(
        topic_name,
        subscription_name,
        identifier={'sequence_number':None,
                    'lock_token':None}):
        '''Remove a locked message of a topic_name beloging to a subscription_name.
        Mesage is referred via sequence_number and/or lock_token retrieved
        during peekMessage/peek_lock_subscription_message
        '''
        self.bus_service.unlock_subscription_message(
            topic_name=topic_name,
            subscription_name=subscription_name,
            sequence_number = identifier['sequence_number'],
            lock_token = identifier['lock_token']
        )
