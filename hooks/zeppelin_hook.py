#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from airflow.hooks.base_hook import BaseHook
from pyzeppelin import ClientConfig, ZeppelinClient, ZSession
import logging


class ZeppelinHook(BaseHook):
    
    def __init__(self, z_conn):
        """

        :param z_conn: Zeppelin connection
        """
        self.z_conn = z_conn
        zeppelin_url = "http://" + self.z_conn.host + ":" + str(self.z_conn.port)
        self.client_config = ClientConfig(zeppelin_url, query_interval=5)
        self.z_client = ZeppelinClient(self.client_config)
        if z_conn.login and z_conn.password:
           self.z_client.login(z_conn.login, z_conn.password)

    @classmethod
    def get_hook(cls, conn_id='zeppelin_default'):
        z_conn = cls.get_connection(conn_id)
        return ZeppelinHook(z_conn=z_conn)

    def run_note(self, note_id, user = None, params = {}, clone_note = True):
        """
        :param note_id:
        :param user:
        :param params: parameters for running this note
        :param clone_note: whether clone a new note and run this cloned note
        """
        note_result = self.z_client.execute_note(note_id, user, params, clone_note)
        if not note_result.is_success():
            raise Exception("Fail to run note, error message: {}".format(note_result.get_errors()))
        else:
            logging.info("note {} is executed successfully".format(note_id))
            logging.info("associated job urls: " + str(list(map(lambda p : (p.paragraph_id, p.jobUrls), note_result.paragraphs))))  

    def run_paragraph(self, note_id, paragraph_id, user = None, params = {}, clone_note = True, isolated = False):
        """

        """
        paragraph_result = self.z_client.execute_paragraph(note_id, paragraph_id, user = user, params = params, clone_note = clone_note, isolated = isolated)
        if not paragraph_result.is_success():
            raise Exception("Fail to run note, error message: {}".format(paragraph_result.get_errors()))
        else:
            logging.info("paragraph {} of note {} is executed successfully".format(paragraph_id, note_id))
            logging.info("associated job urls: " + str(paragraph_result.jobUrls))

    def run_code(self, interpreter, code, sub_interpreter = '', intp_properties = {}):
        """

        """
        z_session = ZSession(self.client_config, interpreter, intp_properties)
        result = z_session.execute(code, sub_interpreter)
        if result.is_success:
            logging.info('Run successfully')
        else:
            raise Exception('Fail to run code, execute_result: {}'.format(result))


