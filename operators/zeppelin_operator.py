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
from airflow.contrib.hooks.zeppelin_hook import ZeppelinHook
from airflow.utils.operator_helpers import context_to_airflow_vars
import logging

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
                 create_cluster_task_id = None,
                 xcom_cluster_id_key = 'xcom_cluster_id',
                 *args,
                 **kwargs):
        super(ZeppelinOperator, self).__init__(*args, **kwargs)
        self.note_id = note_id
        self.paragraph_id = paragraph_id
        self.user = user
        self.cluster_id = cluster_id
        self.params = params
        self.clone_note = clone_note
        self.isolated=isolated
        self.create_cluster_task_id = create_cluster_task_id
        self.xcom_cluster_id_key = xcom_cluster_id_key
        self.z_hook = ZeppelinHook.get_hook(conn_id)

    def execute(self, context):
        params = self.params
        logging.info("context:" + str(context['dag'].default_args))
        self.cluster_id = self.cluster_id if self.cluster_id != None else context['ti'].xcom_pull(task_ids=self.create_cluster_task_id, key=self.xcom_cluster_id_key)
        if self.cluster_id == None and 'cluster_id' in context['dag'].default_args:
            self.cluster_id = context['dag'].default_args['cluster_id']

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        params.update(airflow_context_vars)
        if self.note_id:
            if self.paragraph_id:
                self.z_hook.run_paragraph(self.note_id, self.paragraph_id, self.user, params, self.clone_note, self.cluster_id, True)
            else:
                self.z_hook.run_note(self.note_id, self.user, params, self.clone_note, self.cluster_id)
        else:
            if not self.interpreter:
                raise Exception('interpreter is not specified')
            if not self.code:
                raise Exception('code is not specified')
            self.z_hook.run_code(self.interpreter, self.code, self.intp_properties)
