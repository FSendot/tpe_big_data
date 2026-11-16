from pyspark.sql import SparkSession


def create_spark(settings):
    return (
        SparkSession.builder.appName("cloud-provider-environment")
        .master(settings.master)
        .config("spark.driver.memory", settings.driver_memory)
        .config("spark.sql.session.timeZone", settings.timezone)
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .config("spark.sql.warehouse.dir", (settings.lake_root / "warehouse").as_uri())
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
