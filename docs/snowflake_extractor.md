# MySql Snowflake Extractor

Connects to a database, explores the relations, and extracts the fact table as JSON documents

## Objective

A database can be a complex graph of relations, but there is usually only a few tables that represent the database's facts. This software de-normalizes the database, with respect to these *fact tables*, which results in the hierarchical set of relations that we will call a "snowflake" schema.

The snowflake schema is used to generate a JSON document for each record in the fact table. Many-to-one relations (lookup tables) are represented as inner objects, One-to-many relations (children) are represented as nested objects.

## Denormalization

The denormalization process involves walking all foreign key paths, breadth first and without cycles, from the fact table outward.  This creates a snowflake subset-schema from the original database. This walk requires some guidance to make the JSON beautiful.

Each fact table uses a configuration file to control the denormalization process. I will use the Treeherder job extract configuration as an example:  
 

### Configuration

The `snowflake` object limits the relational walk used to determine the JSON document shape. Without adding limits, all unique relation paths will be traversed, resulting in large, and possibly redundant, documents. You can `exclude` tables entirely, or declare some tables are good for `reference_only`.  

	"snowflake": {
		"fact_table": "job",
		"show_foreign_keys": false,
		"null_values": [
			"-",
			"unknown",
			""
		],
		"add_relations": [
			"treeherder.job.option_collection_hash -> treeherder.option_collection.option_collection_hash"
		],
		"include": [
		],
		"exclude": [
			"runnable_job",
			"auth_user",
			"job_log",
			"text_log_step",
			"performance_datum",
			"performance_alert_summary",
			"performance_signature"
		],
		"reference_only": [
			"user.email",
			"repository.name",
			"machine_platform.platform",
			"failure_classification.name",
			"option.name"
		],
		"database": {
			"schema": "treeherder",
			"username": "activedata",
			"$ref": "~/private.json#treeherder"
		}

* **`fact_table`** - *required* - name of the table that represents the facts being pulled. `snowflake_extractor` can generate nested documents, so you need not choose the finest grain if you are fine with large documents.  For Treeherder, we are interested in the `job` facts.
* **`show_foreign_keys`** - *default true* - Include the foreign key ids. This is useful if you require those ids for later synchronization. If you are only interested in the relationships, then they can be left out, and the JSON will be simpler.
* **`null_values`** - Some databases use a variety of values that indicate *no value*. The database `NULL` is always considered missing, and these values are mapped to `NULL` too.
* **`add_relations`** -  Relations are important for the denormalization. If your database is missing relations, you can add them here. They must be in `<schema>.<table>.<column>` form. Most missing relations are ones that cross schema boundaries; `snowflake_extractor` can reach across those boundaries for complete denormalization.
* **`name_relations`** - By default many-to-one relations use the column name for the property, while one-to-many relations use the referenced table name. You can change this to more convenient names, or remove ambiguity by adding relations and their respective names. Use `->` for many-to-one and `<-` for one-to-many.
* **`exclude`** - Some tables are not needed: They may be irrelevant for the extraction process, or they may contain sensitive information, or you may not have permissions to access the contents. In all these cases, the tables can be added to this list. For the Treeherder example, there are many `exclude` entries; this is to avoid pulling the Perfherder facts, which we pull using separate configuration. You may exclude just a single column using `<table>.<column>` format
* **`exclude_path`** - Some paths lead to lookup table pointed to by other facts. These facts are desired, just not in this path. This also excludes subpaths.
* **`reference_only`** - *`<table>.<column>`* - Some tables are used to lookup primitive values, or maybe you are not interested in the properties for a given table: In these cases you can have the foreign key replaced with the canonical value that foreign key represents. For example: `users.email`, everywhere there is a `user_id` column, the foreign key is replaced with the `email` value. This greatly simplifies the JSON at the risk of loosing some information. 
* **`reference_only`** - *`<table>`* - If just the table is named, then it is included with all its columns, but no nested documents will be attached to it, or any of its inner objects.   
* **`database`** - properties required to connect to the database. Must include `schema` so that the `fact_table` name has context.

## Using Trace 

To turn on the trace, you enable debugging by adding the following property to the config file:

	"constants":{
		"jx_mysql.mysql_snowflake_extractor.DEBUG": true
	},
  
During a run, the "trace" will show all paths being traversed. In the example below, I left out some  `exclude` tables, and you can see the resulting pathology: The `push` table is used by both the `job` fact table, and the `performance_alert_summary` table; the foreign keys provide a path from `job` to `push.performance_alert_summary`.  If you want every push to include all the alert summaries that are related, you can certainly keep them, but for this scenario, it is too deep; much better to extract the `performance_alert_summary` separately. For this reason we exclude the `performance*` tables.


	Trace .
	Trace job
	Trace job.failure_classification
	Trace job.option_collection
	Trace job.reference_data_signatures
	Trace job.build_platform
	Trace job.job_type
	Trace job.machine
	Trace job.machine_platform
	Trace job.product
	Trace job.push
	Trace job.repository
	Trace job.performance_datum
	Trace job.performance_datum
	Trace job.job_detail
	Trace job.bug_job_map
	Trace job.job_note
	Trace job.option_collection.option
	Trace job.option_collection.performance_signature
	Trace job.job_type.job_group
	Trace job.push.repository
	Trace job.push.commit
	Trace job.push.performance_alert_summary
	Trace job.push.performance_alert_summary
	Trace job.push.performance_datum
	Trace job.performance_datum.performance_signature
	Trace job.performance_datum.repository
	Trace job.job_note.failure_classification
	Trace job.option_collection.performance_signature.performance_signature
	Trace job.option_collection.performance_signature.performance_framework
	Trace job.option_collection.performance_signature.machine_platform
	Trace job.option_collection.performance_signature.repository
	Trace job.option_collection.performance_signature.performance_alert
	Trace job.push.performance_alert_summary.performance_framework
	Trace job.push.performance_alert_summary.repository
	Trace job.push.performance_alert_summary.performance_alert
	Trace job.push.performance_alert_summary.performance_alert
	Trace job.option_collection.performance_signature.performance_framework
	

Here is what it looks like when the `commit` is not excluded.

    "push": {
        "author": "jjong@mozilla.com",
        "commit": [
            {
                "author": "Jessica Jong <jjong@mozilla.com>",
                "comments": "Bug 1321245 - request native key bindings for delayed keypress events. r=smaug",
                "revision": "a8aec29e89a344aa6e14b393aacbfaef2ec6f86e"
            },
            {
                "author": "Jessica Jong <jjong@mozilla.com>",
                "comments": "try: -b do -p all -u all -t none",
                "revision": "065343694038fb04c3fa51223589206a0dcdbd07"
            }
        ],
        "repository": "try",
        "revision": "065343694038fb04c3fa51223589206a0dcdbd07",
        "revision_hash": "1d86ae4d89624716f7e25776734fecd20743da56",
        "time": 1480663991
    }

The commit information is readily available from ActiveData, so we do not need to repeat ourselves here. But, this is a personal call; hg.mozilla.org will eventually erase its knowledge of try pushes, so if you need the try comments maybe you should keep them here.  Alternately, you could consider the `commit` table yet-another-fact-table for separate extraction.  
