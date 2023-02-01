{% autoescape false %}
# IMPORTS START
{% if imports is defined %}{{imports}}{%- endif -%}
{{DEFAULT_IMPORTS}}
{% if 'PYSPARK' == type %}
{{PYSPARK_DEFAULTS}}
{%- endif -%}
{% if 'PYSHELL' == type %}
{{PYSHELL_DEFAULTS}}
{%- endif -%}
# IMPORTS END


# ARG START
args = getResolvedOptions(sys.argv, [{% for line in args %}{%- if loop.last -%}"{{line}}"{% else -%}"{{line}}", {% endif %}{% endfor %}])

{% for line in args %}
{% filter lower %}{{ line | replace("-", "_") }}{% endfilter %} = args["{{ line | replace("-", "_") }}"]
{% endfor %}
# ARG END


{% if 'PYSPARK' == type -%}
# SPARK START
{% if spark is defined %}
{{spark}}
{%- else %}
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
{%- endif %}

# SPARK END
{%- endif %}


# LOG START
{% if 'PYSHELL' == type %}
currenttime = commonutil.getdaytime()
logger = log.TahoeLogger().getPyshellLogger(continuous_log_loggroup, currenttime)
{% endif %}
{%- if 'PYSPARK' == type %}
logger = log.TahoeLogger().getSparkLogger(glueContext.get_logger())
{% endif %}
# LOG END


# CODE START
{% if code is defined %}
{{code}}
{%- endif %}
# CODE END
{% endautoescape %}