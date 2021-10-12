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


from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.zeppelin_hook import ZeppelinHook
from airflow.utils.operator_helpers import context_to_airflow_vars
import logging
import time


class ZeppelinOperator(BaseOperator):

    template_fields = ('params', )

    @apply_defaults
    def __init__(self,
                 conn_id,
                 note_id,
                 paragraph_id = None,
                 user = None,
                 cluster_id = None,
                 params={},
                 clone_note=True,
                 isolated=True,
                 *args,
                 **kwargs):
        super(ZeppelinOperator, self).__init__(*args, **kwargs)
        self.note_id = note_id
        self.exec_note_id = note_id
        self.paragraph_id = paragraph_id
        self.user = user
        self.cluster_id = cluster_id
        self.params = params
        self.clone_note = clone_note
        self.isolated=isolated
        self.z_hook = ZeppelinHook.get_hook(conn_id)

    def execute(self, context):
        params = self.params
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        if self.note_id:
            self.z_hook.refresh_note(self.note_id)
            try:
                if self.clone_note:
                    note_json = self.z_hook.get_note(self.note_id)
                    note_name = note_json['name']
                    dest_note_path = '/airflow_jobs/' + note_name + "/" + note_name + '_' + airflow_context_vars['AIRFLOW_CTX_EXECUTION_DATE'].replace(':','-')
                    self.exec_note_id = self.z_hook.clone_note(self.note_id, dest_note_path)
                if self.paragraph_id:
                    self.z_hook.run_paragraph(self.exec_note_id, self.paragraph_id, params, True)
                else:
                    self.z_hook.run_note(self.exec_note_id, params)
            finally:
                if self.clone_note and self.exec_note_id != self.note_id:
                    self.z_hook.delete_note(self.exec_note_id)
        else:
            if not self.interpreter:
                raise Exception('interpreter is not specified')
            if not self.code:
                raise Exception('code is not specified')
            self.z_hook.run_code(self.interpreter, self.code, self.intp_properties)

    def on_kill(self):
        if self.note_id:
            if self.paragraph_id:
                logging.info("kill paragraph: " + self.paragraph_id)
                self.z_hook.stop_paragraph(self.exec_note_id, self.paragraph_id)
            else:
                logging.info("kill note: " + self.exec_note_id)
                self.z_hook.stop_note(self.exec_note_id)
        time.sleep(3)
        ZeppelinOperator.on_kill(self)
