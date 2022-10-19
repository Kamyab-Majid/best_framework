# Glue interactive session for jupyter

Here we discuss the procedure to integrate a glue session in jupyter. Glue interactive sessions is for debugging on a small datasets,  please refer to the provided links in this document if the procedure is deprecated and update this document accordingly.

## debugging environment

Consider setting up a [virtual environment](https://docs.python.org/3/tutorial/venv.html) before using this feature.

## Python package Installation

Depending on your environment, you may need to run one of the following commands based on [glue_interactive sessions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html):

``` bash
pip3 install --user --upgrade jupyter boto3 aws-glue-sessions
```

or

``` bash
pip install --user --upgrade jupyter boto3 aws-glue-sessions
```

## adding glue_spark extensions for jupyter

The following commands will add glue_spark/glue_pyspark extensions to jupyter based on [glue_interactive sessions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html).

```bash
SITE_PACKAGES=$(pip3 show aws-glue-sessions | grep Location | awk '{print $2}')
jupyter kernelspec install $SITE_PACKAGES/aws_glue_interactive_sessions_kernel/glue_pyspark --user
jupyter kernelspec install $SITE_PACKAGES/aws_glue_interactive_sessions_kernel/glue_spark --user
```

## AWS CLI installation and configuration

### Installation

according to [getting started with AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html),
to install the AWS CLI use following commands:

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws configure
```

Use your IAM role or user to configure the aws.

### configure aws using credentials

After installation, the command `aws sts get-caller-identity` should result in one of the followings:

```json
{
    "UserId": "THE_USER_ID",
    "Account": "ACCOUNT_NO",
    "Arn": "arn:aws:iam::123456789123:role/myIAMRole"
}
```

```json
{
    "UserId": "THE_USER_ID",
    "Account": "123456789123",
    "Arn": "arn:aws:iam::123456789123:user/MyIAMUser"
}

```

Which depends on if you set your aws for a user or based on a role. In first case which is when it is a role use:

```bash
aws iam attach-role-policy --role-name myIAMRole  --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess
```

in the other case use:

```bash
aws iam attach-user-policy --user-name MyIAMUser --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess
```

Which would give you enough access to use this feature.

## Prepare the AWS Glue service role for interactive sessions

You can specify the second principal, GlueServiceRole, either in the notebook itself by using the `%iam_role` magic or stored alongside the AWS CLI config. If you have a role that you typically use with AWS Glue jobs, this will be that role. If you don’t have a role you use for AWS Glue jobs, refer to [Setting up IAM permissions for AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html) to set one up.
To edit the aws config file use the following, you can change code with the editor of your choice (for example `nano`):

### using credentials file

```bash
code ~/.aws/credentials
```

add the following to your config file:

```txt
glue_role_arn=arn:aws:iam::Account:role/service-role/<ROLE>
region=<defualt_region>
```

in which `ARN` is the [ARN](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html) of the role you made to be used for glue. You can find it using [IAM console](https://console.aws.amazon.com/iam/home) by searching the role name or using CLI:

```bash
aws iam get-role --role-name <role name>
```

### Using jupyter magic role

You can use multiple magic roles in the session in which a complete list of them are given in [Interactive session magic](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions-magics.html) for lower cost we recommend the following magics:
Please have in mind that the below magics should be run **first before other cells are run** otherwise it will not affect the current session.
| Name                       | Type       | Description                                                                                                                                                      |
|----------------------------|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| %%configure                | Dictionary | Specify a JSON-formatted dictionary consisting of all configuration parameters for a session. Each parameter can be specified here or through individual magics. |
| %iam_role                  | String     | Specify an IAM role ARN to execute your session with. Default from ~/.aws/configure                                                                              |
| %number_of_workers         | int        | The number of workers of a defined worker_type that are allocated when a job runs. worker_type must be set too. The default number_of_workers is 5.              |
| %worker_type               | String     | Standard, G.1X, or G.2X. number_of_workers must be set too. The default worker_type is G.1X.                                                                     |
| %security_config           | String     | Define a Security Configuration to be used with this session.                                                                                                    |
| %connections               | List       | Specify a comma-separated list of connections to use in the session.                                                                                             |
| %additional_python_modules | List       | Comma separated list of additional Python modules to include in your cluster (can be from Pypi or S3).                                                           |
| %extra_py_files            | List       | Comma separated list of additional Python files from Amazon S3.                                                                                                  |
| %extra_jars                | List       | Comma-separated list of additional jars to include in the cluster.|
| %idle_timeout              | int         | The number of minutes of inactivity after which a session will timeout after a cell has been executed. The default idle timeout value is `2880` (minutes).|

The following options are available after each session is initiated.

| Name               | Type   | Description                                                                                                                                                         |
|--------------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| %help              | n/a    | Return a list of descriptions and input types for all magic commands.                                                                                               |
| %profile           | String | Specify a profile in your AWS configuration to use as the credentials provider.                                                                                     |
| %region            | String | Specify the AWS Region; in which to initialize a session. Default from ~/.aws/configure.                                                                            |
| %idle_timeout      | Int    | The number of minutes of inactivity after which a session will timeout after a cell has been executed. The default idle timeout value is 2880 minutes (48 hours).   |
| %session_id        | String | Return the session ID for the running session. If a String is provided, this will be set as the session ID for the next running session.                            |
| %session_id_prefix | String | Define a string that will precede all session IDs in the format [session_id_prefix]-[session_id]. If a session ID is not provided, a random UUID will be generated. |
| %status            |        | Return the status of the current AWS Glue session including its duration, configuration and executing user / role.                                                  |
| %stop_session      |        | Stop the current session.                                                                                                                                           |
| %list_sessions     |        | Lists all currently running sessions by name and ID.                                                                                                                |
| %glue_version      | String | The version of Glue to be used by this session. Currently, the only valid options are 2.0 and 3.0. The default value is 2.0.                                        |
| %streaming         | String | Changes the session type to AWS Glue Streaming.                                                                                                                     |
| %etl               | String | Changes the session type to AWS Glue ETL.                                                                                                                           |

```python
%stop_session
%glue_version 3.0
%number_of_workers 2
%worker_type G.2X
%idle_timeout 1
```

each session is run after you run anything but the magic commands.
> **_NOTE:_**  Make sure to use `%idle_timeout` before each session is run and also to use `%stop_session` to avoid high costs. The cost is based on the number of workers per hour (0.44$ per hour) so also using least (2) `%number_of_workers 2` is recommended.

When you run magics, the output lets us know the values we’re changing along with their previous settings. Explicitly setting all your configuration in magics helps ensure consistent runs of your notebook every time and is recommended for production workloads.

> **_NOTE:_** Make sure to set the Kernel on glue pyspark before using jupyter,  If you want to use vscode or other IDE, start the jupyter notebook and then connect the IDE to the jupyter notebook. This procedure is shown for vscode in the [connect to a remote jupyter server](https://code.visualstudio.com/docs/datascience/jupyter-notebooks#_connect-to-a-remote-jupyter-server). Make sure to install [jupyter extension](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter) first.

## Sample data manipulation in interactive session

In the rest of this document we have given an example of how to work with jupyter glue pyspark interactive sessions[^1].
[^1]: credit to Zach Mitchell from website [introducing aws-glue interactive session](https://aws.amazon.com/blogs/big-data/introducing-aws-glue-interactive-sessions-for-jupyter/)
</div>
       <h3>Run your first code cell and author your AWS Glue notebook</h3>
       <p>Next, we run our first code cell. This is when a session is provisioned for use with this notebook. When interactive sessions are <a href="https://docs.aws.amazon.com/glue/latest/dg/glue-is-security.html#glue-is-tagoncreate" target="_blank" rel="noopener noreferrer">properly configured</a> within an account, the session is completely isolated to this notebook. If you open another notebook in a new tab, it gets its own session on its own isolated compute. Run your code cell as follows:</p>
```python
%stop_session
%glue_version 3.0
%number_of_workers 2
%worker_type G.2X
%idle_timeout 1
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

```

</div> 
<p>When you ran the first cell containing code, Jupyter invoked interactive sessions, provisioned an AWS Glue cluster, and sent the code to AWS Glue Spark. The notebook was given a session ID, as shown in the preceding code. We can also see the properties used to provision AWS Glue, including the IAM role that AWS Glue used to create the session, the number of workers and their type, and any other options that were passed as part of the creation.</p> 
<p>Interactive sessions automatically initialize a Spark session as <code>spark</code> and <code>SparkContext</code> as <code>sc;</code> having Spark ready to go saves a lot of boilerplate code. However, if you want to convert your notebook to a job, <code>spark</code> and <code>sc</code> must be initialized and declared explicitly.</p> 
<h3>Work in the notebook</h3> 
</div>
       <p>When I’m working on a new data integration process, the first thing I often do is identify and preview the datasets I’m going to work on. If I don’t recall the exact location or table name, I typically open the AWS Glue console and search or browse for the table then return to my notebook to preview it. With interactive sessions, there is a quicker way to browse the Data Catalog. We can use the <code>%%sql</code> magic to show databases and tables without leaving the notebook. For this example, the population table I want in is the COVID-19 dataset but I don’t recall its exact name, so I use the <code>%%sql</code> magic to look it up:</p> 
       <div class="hide-language"> 
        <pre><code class="lang-sql">%%sql
show tables in `covid-19`  # Remember, dashes in names must be escaped with backticks.

+--------+--------------------+-----------+
|database|           tableName|isTemporary|
+--------+--------------------+-----------+
|covid-19|alleninstitute_co...|      false|
|covid-19|alleninstitute_me...|      false|
|covid-19|aspirevc_crowd_tr...|      false|
|covid-19|aspirevc_crowd_tr...|      false|
|covid-19|cdc_moderna_vacci...|      false|
|covid-19|cdc_pfizer_vaccin...|      false|
|covid-19|       country_codes|      false|
|covid-19|  county_populations|      false|
|covid-19|covid_knowledge_g...|      false|
|covid-19|covid_knowledge_g...|      false|
|covid-19|covid_knowledge_g...|      false|
|covid-19|covid_knowledge_g...|      false|
|covid-19|covid_knowledge_g...|      false|
|covid-19|covid_knowledge_g...|      false|
|covid-19|covid_testing_sta...|      false|
|covid-19|covid_testing_us_...|      false|
|covid-19|covid_testing_us_...|      false|
|covid-19|      covidcast_data|      false|
|covid-19|  covidcast_metadata|      false|
|covid-19|enigma_aggregatio...|      false|
+--------+--------------------+-----------+
only showing top 20 rows</code></pre> 
       </div> 
       <p>Looking through the returned list, we see a table named <code>county_populations</code>. Let’s select from this table, sorting for the largest counties by population:</p> 
       <div class="hide-language"> 
        <pre><code class="lang-sql">%%sql
select * from `covid-19`.county_populations sort by `population estimate 2018` desc limit 10

+--------------+-----+---------------+-----------+------------------------+
|            id|  id2|         county|      state|population estimate 2018|
+--------------+-----+---------------+-----------+------------------------+
|            Id|  Id2|         County|      State|    Population Estima...|
|0500000US01085| 1085|        Lowndes|    Alabama|                    9974|
|0500000US06057| 6057|         Nevada| California|                   99696|
|0500000US29189|29189|      St. Louis|   Missouri|                  996945|
|0500000US22021|22021|Caldwell Parish|  Louisiana|                    9960|
|0500000US06019| 6019|         Fresno| California|                  994400|
|0500000US28143|28143|         Tunica|Mississippi|                    9944|
|0500000US05051| 5051|        Garland|   Arkansas|                   99154|
|0500000US29079|29079|         Grundy|   Missouri|                    9914|
|0500000US27063|27063|        Jackson|  Minnesota|                    9911|
+--------------+-----+---------------+-----------+------------------------+</code></pre> 
       </div> 
       <p>Our query returned data but in an unexpected order. It looks like <code>population estimate 2018</code> sorted lexicographically if the values were strings. Let’s use an AWS Glue DynamicFrame to get the schema of the table and verify the issue:</p> 
       <div class="hide-language"> 
        <pre><code class="lang-sql"># Create a DynamicFrame of county_populations and print it's schema
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="covid-19", table_name="county_populations"
)
dyf.printSchema()

root
|-- id: string
|-- id2: string
|-- county: string
|-- state: string
|-- population estimate 2018: string</code></pre> 
       </div> 
       <p>The schema shows <code>population estimate 2018</code> to be a string, which is why our column isn’t sorting properly. We can use the <a href="https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-transforms-ApplyMapping.html" target="_blank" rel="noopener noreferrer">apply_mapping</a> transform in our next cell to correct the column type. In the same transform, we also clean up the column names and other column types: clarifying the distinction between <code>id</code> and <code>id2</code>, removing spaces from <code>population estimate 2018</code> (conforming to Hive’s standards), and casting <code>id2</code> as an integer for proper sorting. After validating the schema, we show the data with the new schema:</p> 
       <div class="hide-language"> 
        <pre><code class="lang-sql"># Rename id2 to simple_id and convert to Int
# Remove spaces and rename population est. and convert to Long
mapped = dyf.apply_mapping(
    mappings=[
        ("id", "string", "id", "string"),
        ("id2", "string", "simple_id", "int"),
        ("county", "string", "county", "string"),
        ("state", "string", "state", "string"),
        ("population estimate 2018", "string", "population_est_2018", "long"),
    ]
)
mapped.printSchema()
 
root
|-- id: string
|-- simple_id: int
|-- county: string
|-- state: string
|-- population_est_2018: long


mapped_df = mapped.toDF()
mapped_df.show()

+--------------+---------+---------+-------+-------------------+
|            id|simple_id|   county|  state|population_est_2018|
+--------------+---------+---------+-------+-------------------+
|0500000US01001|     1001|  Autauga|Alabama|              55601|
|0500000US01003|     1003|  Baldwin|Alabama|             218022|
|0500000US01005|     1005|  Barbour|Alabama|              24881|
|0500000US01007|     1007|     Bibb|Alabama|              22400|
|0500000US01009|     1009|   Blount|Alabama|              57840|
|0500000US01011|     1011|  Bullock|Alabama|              10138|
|0500000US01013|     1013|   Butler|Alabama|              19680|
|0500000US01015|     1015|  Calhoun|Alabama|             114277|
|0500000US01017|     1017| Chambers|Alabama|              33615|
|0500000US01019|     1019| Cherokee|Alabama|              26032|
|0500000US01021|     1021|  Chilton|Alabama|              44153|
|0500000US01023|     1023|  Choctaw|Alabama|              12841|
|0500000US01025|     1025|   Clarke|Alabama|              23920|
|0500000US01027|     1027|     Clay|Alabama|              13275|
|0500000US01029|     1029| Cleburne|Alabama|              14987|
|0500000US01031|     1031|   Coffee|Alabama|              51909|
|0500000US01033|     1033|  Colbert|Alabama|              54762|
|0500000US01035|     1035|  Conecuh|Alabama|              12277|
|0500000US01037|     1037|    Coosa|Alabama|              10715|
|0500000US01039|     1039|Covington|Alabama|              36986|
+--------------+---------+---------+-------+-------------------+
only showing top 20 rows</code></pre> 
       </div> 
       <p>With the data sorting correctly, we can write it to <a href="http://aws.amazon.com/s3" target="_blank" rel="noopener noreferrer">Amazon Simple Storage Service</a> (Amazon S3) as a new table in the AWS Glue Data Catalog. We use the mapped DynamicFrame for this write because we didn’t modify any data past that transform:</p> 
       <div class="hide-language"> 
        <pre><code class="lang-bash"># Create "demo" Database if none exists
spark.sql("create database if not exists demo")


# Set glueContext sink for writing new table
S3_BUCKET = "&lt;S3_BUCKET&gt;"
s3output = glueContext.getSink(
    path=f"s3://{S3_BUCKET}/interactive-sessions-blog/populations/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
)
s3output.setCatalogInfo(catalogDatabase="demo", catalogTableName="populations")
s3output.setFormat("glueparquet")
s3output.writeFrame(mapped)


# Write out ‘mapped’ to a table in Glue Catalog
s3output = glueContext.getSink(
    path=f"s3://{S3_BUCKET}/interactive-sessions-blog/populations/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
)
s3output.setCatalogInfo(catalogDatabase="demo", catalogTableName="populations")
s3output.setFormat("glueparquet")
s3output.writeFrame(mapped)</code></pre> 
       </div> 
       <p>Finally, we run a query against our new table to show our table created successfully and validate our work:</p> 
       <div class="hide-language"> 
        <pre><code class="lang-sql">%%sql
select * from demo.populations</code></pre> 
       </div> 
       <h2>Convert notebooks to AWS Glue jobs with nbconvert</h2> 
       <p>Jupyter notebooks are saved as .ipynb files. AWS Glue doesn’t currently run .ipynb files directly, so they need to be converted to Python scripts before they can be uploaded to Amazon S3 as jobs. Use the <code>jupyter nbconvert</code> command from a terminal to convert the script.</p> 
       <ol> 
        <li>Open a new terminal or PowerShell tab or window.</li> 
        <li><code>cd</code> to the working directory where your notebook is.<br> This is likely the same directory where you ran jupyter notebook at the beginning of this post.</li> 
        <li>Run the following bash command to convert the notebook, providing the correct file name for your notebook: 
         <div class="hide-language"> 
          <pre><code class="lang-python">jupyter nbconvert --to script &lt;Untitled-1&gt;.ipynb</code></pre> 
         </div> </li> 
        <li>Run <code>cat &lt;Untitled-1&gt;.ipynb</code> to view your new file.</li> 
        <li>Upload the .py file to Amazon S3 using the following command, replacing the bucket, path, and file name as needed: 
         <div class="hide-language"> 
          <pre><code class="lang-python">aws s3 cp &lt;Untitled-1&gt;.py s3://&lt;bucket&gt;/&lt;path&gt;/&lt;Untitled-1.py&gt;</code></pre> 
         </div> </li> 
        <li>Create your AWS Glue job with the following command.</li> 
       </ol> 
       <p>Note that the magics aren’t automatically converted to job parameters when converting notebooks locally. You need to put in your job arguments correctly, or import your notebook to AWS Glue Studio and complete the following steps to keep your magic settings.</p> 
       <div class="hide-language"> 
        <pre><code class="lang-bash">aws glue create-job \
    --name is_blog_demo
    --role "&lt;GlueServiceRole&gt;" \
    --command {"Name": "glueetl", "PythonVersion": "3", "ScriptLocation": "s3://&lt;bucket&gt;/&lt;path&gt;/&lt;Untitled-1.py"} \
    --default-arguments {"--enable-glue-datacatalog": "true"} \
    --number-of-workers 2 \
    --worker-type G.2X</code></pre> 
       </div> 
       <h2>Run the job</h2> 
       <p>After you have authored the notebook, converted it to a Python file, uploaded it to Amazon S3, and finally made it into an AWS Glue job, the only thing left to do is run it. Do so with the following terminal command:</p> 
       <div class="hide-language"> 
        <pre><code class="lang-bash">aws glue start-job-run --job-name is_blog --region us-east-1</code></pre> 
       </div> 
       <h2>Conclusion</h2> 
       <p>AWS Glue interactive sessions offer a new way to interact with the AWS Glue serverless Spark environment. Set it up in minutes, start sessions in seconds, and only pay for what you use. You can use interactive sessions for AWS Glue job development, ad hoc data integration and exploration, or for large queries and audits. AWS Glue interactive sessions are generally available in all Regions that support AWS Glue.</p> 
       <p>To learn more and get started using AWS Glue Interactive Sessions visit our <a href="https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html" target="_blank" rel="noopener noreferrer">developer guide</a> and begin coding in seconds.</p>
