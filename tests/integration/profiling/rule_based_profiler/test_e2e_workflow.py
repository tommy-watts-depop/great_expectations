# I think this should have been here to begin with
import logging
from typing import List

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from ruamel import yaml

import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.util import get_or_create_spark_application
from great_expectations.rule_based_profiler.data_assistant import (
    DataAssistant,
    VolumeDataAssistant,
)
from great_expectations.rule_based_profiler.data_assistant_result import (
    VolumeDataAssistantResult,
)
from great_expectations.validator.validator import Validator

schema = StructType(
    [
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("rate_code_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pickup_location_id", IntegerType(), True),
        StructField("dropoff_location_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
    ]
)

data_path: str = "../../../../test_sets/taxi_yellow_tripdata_samples"


# data_context.test_yaml_config(yaml.dump(datasource_config))


@pytest.mark.integration
def test_spark_example(empty_data_context):
    data_context = empty_data_context
    logger = logging.getLogger(__name__)
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        logger.debug(
            "Unable to load spark context; install optional spark dependency for support."
        )
    spark = get_or_create_spark_application()
    data_path: str = "/Users/work/Development/great_expectations/tests/test_sets/taxi_yellow_tripdata_samples/"
    import os

    cwd = os.getcwd()
    # one reason this might not work:
    # it has to be serialized and de-serialized.. and we can't guarantee that step?
    # can we make an execution engine with the right thing?
    print(f"class: of schema {type(schema)}")
    datasource_config: dict = {
        "name": "taxi_data",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SparkDFExecutionEngine",
        },
        "data_connectors": {
            "configured_data_connector_multi_batch_asset": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": data_path,
                "assets": {
                    "yellow_tripdata_2019": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2019)-(\\d.*)\\.csv",
                        "batch_spec_passthrough": {
                            "reader_method": "csv",
                            "reader_options": {
                                "schema": schema,
                            },
                        },
                    },
                    "yellow_tripdata_2020": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2020)-(\\d.*)\\.csv",
                        "batch_spec_passthrough": {
                            "reader_method": "csv",
                            "reader_options": {
                                "schema": schema,
                            },
                        },
                    },
                },
            },
        },
    }

    data_context.test_yaml_config(yaml.dump(datasource_config))

    try:
        data_context.get_datasource(datasource_config["name"])
    except ValueError:
        data_context.add_datasource(**datasource_config)

    multi_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2019",
    )
    data_context
    batch_list = data_context.get_batch_list(batch_request=multi_batch_batch_request)
