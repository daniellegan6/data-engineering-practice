import great_expectations as gx
from pyspark.sql.functions import date_format, col


def check_data_quality(df):
    # Get the context
    context = gx.get_context()

    # Create a data source
    data_source_name = "bike_trips"
    data_source = context.data_sources.add_spark(name=data_source_name)

    # Create a data asset
    data_asset_name = "trips"
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

    # Build a batch request
    batch_definition_name = "daily_trips"
    batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)
    
    # Create batch parameters
    batch_parameters = {"dataframe": df}

    # Create an expectation suite
    expectation_suite_name = "bike_trips_suite"
    suite = gx.ExpectationSuite(name=expectation_suite_name)
    # Add the Expectation Suite to the Data Context
    suite = context.suites.add(suite)


    # Add expectations to the Expectation Suite
    # 1. Check that duration is not unreasonably long (24 hours = 86400 seconds)
    expectation = gx.expectations.ExpectColumnValuesToBeBetween(
            column="duration_seconds",
            min_value=0,
            max_value=86400,
            mostly=0.99
    )
    suite.add_expectation(expectation)

    # 2. Check that the trip start and end on the same day
    expectation = gx.expectations.ExpectColumnPairValuesToBeEqual(
        column_A="start_date",
        column_B="end_date",
        mostly=0.95
    )
    suite.add_expectation(expectation)

    # Create a Validation Definition
    validation_definition_name = "bike_trips_validation"
    validation_definition = gx.ValidationDefinition(
        data=batch_definition, 
        suite=suite, 
        name=validation_definition_name
    )

    # Save the Validation Definition to your Data Context.
    validation_definition = context.validation_definitions.add(validation_definition)

    # Run validation
    validation_results = validation_definition.run(batch_parameters=batch_parameters)
    print(validation_results)

    # Check if any validations failed
    if not validation_results.success:
        error_mesage = "Data quality checks failes:\n"
        for result in validation_results.results:
            if not result.success:
                error_mesage += f" - {result.expectation_config.expectation_type}: "
                error_mesage += f"Expected {result.expectation_config_kwargs}, "
                error_mesage += f"but found {result.result['unexpected_count']} unexpected values\n"
            raise Exception(error_mesage)