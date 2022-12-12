# EMR Development

## Development

Development process is the same for glue jobs.

## Deployment

The glue job scripts will run inside a correctly configured EMR with pyspark and prerequisite libraries installed.

The deployment and invocation will be different, and allocating workers will be up to the invocation arguments or some
logic/spark configuration on the EMR.

## Glue vs EMR

Amazon EMR and AWS Glue are both cloud-based big data processing services, but they serve different purposes and are
typically used in different scenarios.

- Amazon EMR is a managed big data processing service that makes it easy to set up, operate, and scale a data processing
cluster in the cloud. It is typically used for running large-scale batch jobs, such as data transformations, analytics,
and machine learning algorithms.

- AWS Glue is a fully managed extract, transform, and load (ETL) service that makes it easy to move data between data
stores. It is typically used to move data between data stores in a scalable and reliable way, and to prepare data for
analysis and machine learning.

In general, you should use:

- AWS Glue if you need to move data between data stores and prepare it for analysis
- Amazon EMR if you need to run large-scale batch jobs for data processing and analysis.

It is also possible to use both services together, where AWS Glue is used to move and prepare data, and Amazon EMR is used to process and analyze the data.

### Use Cases

EMR:

- Long-running or continuous processes
- Integration of pipeline steps not suitable for spark (i.e. training/image preprocessing)
- Heavy data analysis, with several intermediate states

Glue:

- Used to preprocess data more economically,
- Glue is more suited to pure ETL and migration activities.

The two can be integrated into a pipeline, especially when data needs to be gathered from multiple sources first - or
continuously, and an EMR process is run in batches.

### Environment Variable Management

Each environment/project will have a JSON config file with ENV variables set.
Both the Glue and EMR are managed by reading the config.
