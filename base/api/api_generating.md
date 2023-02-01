## TAHOE-298

# Problem
We'd like the schema generation that takes place in Redshift to drive the automated doc generation that happens in the API. The APISpec library currently uses static Python classes to introspect and generate the definitions from that. In this paradigm, if the Redshift schema changes, then a developer would have to manually alter the Python class. This isn't ideal as the Redshift schema may change frequently.

# Solution
One posible solution is as follows:
On start up of the lambda function, run the following
1. Run below using the `boto3` library; `list_tables()` [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Client.list_tables)
2. Run below against Redshift using the tables above.
```
select column_name,
  case
    when data_type = 'integer' then 'integer'
    when data_type = 'bigint' then 'bigint'
    when data_type = 'smallint' then 'smallint'
    when data_type = 'text' then 'text'
    when data_type = 'date' then 'date'
    when data_type = 'real' then 'real'
    when data_type = 'boolean' then 'boolean'
    when data_type = 'double precision' then 'float8'
    when data_type = 'timestamp without time zone' then 'timestamp'
    when data_type = 'character' then 'char('||character_maximum_length||')'
    when data_type = 'character varying' then 'varchar('||character_maximum_length||')'
    when data_type = 'numeric' then 'numeric('||numeric_precision||','||numeric_scale||')'
    else 'unknown'
  end as data_type,
  is_nullable,
  column_default
 from information_schema.columns
 where table_schema = '<TABLE_SCHEMA_NAME>' and table_name = '<TABLE_NAME>' order by ordinal_position
;
```
2b. Alternatively, using the `describe_table()` from the `boto3` library according to the [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Client.describe_table) would return the column names and types.
3. Create a dictionary from the results where each key is a string of the property name and it maps to a value that is the type as referenced by `fields.<TYPE>()`. An example from the Marshmallow docs is:
```
from marshmallow import Schema, fields

UserSchema = Schema.from_dict(
    {"name": fields.Str(), "email": fields.Email(), "created_at": fields.DateTime()}
)
```
4. Add the schemas to a master list, and reference it from the `spec.to_flasgger()` method.
5. Create a reference map that maps endpoint to list of definitions created by `Schema.from_dict()`. Use this map to format the doc string in each endpoint.
    a. Use the <STACK_PREFIX> variables present to prefix any table names. 
    b. Instead of formatting the doc string, it may be easier to create a dict according the [flasgger docs](https://github.com/flasgger/flasgger#using-dictionaries-as-raw-specs)


# GraphQL and TAHOE
Here are a couple links found in a mild session of research. The idea is that GraphQL would be a single endpoint in the Lambda function. That endpoind would take a graphql query. Using the following methodologies, writing resolvers to expose the Redshift database would be the primary mode of development. I don't think this would jive well with what was outlined above with auto-generated schemas. Graphql typically requires a static set of schemas. Use this information to inform your decisions.

Start reading with Apollo:
[Apollo Graphql](https://www.apollographql.com/blog/graphql/python/complete-api-guide/) - The graphql "server"
[Python Pypika](https://github.com/kayak/pypika) - Useful for generating the SQL strings required for pulling data from Redshift
[Apischema](https://wyfo.github.io/apischema/0.18/graphql/overview/) - A different ORM for linking graphql to Python classes