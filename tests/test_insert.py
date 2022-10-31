# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import mo_json_config
from jx_mysql.models.container import Container
from mo_logs import startup, constants

from jx_mysql.insert import Facts
from jx_mysql.mysql import MySql
from mo_testing.fuzzytestcase import FuzzyTestCase

settings = startup.read_settings(filename="tests/resources/config/test.json")
constants.set(settings.constants)


class TestInsert(FuzzyTestCase):
    def setUp(self):
        config = mo_json_config.get("file://tests/resources/config/test.json")
        self.db = MySql(kwargs=config.database)
        with self.db.transaction() as t:
            t.execute(f"DROP DATABASE {config.database.schema}")
            t.execute(f"CREATE DATABASE {config.database.schema}")

    def test_insert_docs(self):
        facts = Facts("test_table", Container(self.db))
        facts.insert([{"a": 1, "b": 2}, {"a": "a", "b": "b"}])
        result = self.db.query("SELECT * FROM test_table", format="list")
        expected = [{"a.$N": 1, "b.$N": 2}, {"a.$S": "a", "b.$S": "b"}]
        self.assertEqual(result, expected)
