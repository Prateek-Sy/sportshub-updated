def write_to_snowflake(df, snowflake_config, schema_name, table_name):

        df.write \
            .format("snowflake") \
            .option("sfURL", snowflake_config['sf_url']) \
            .option("sfDatabase", snowflake_config['sf_database']) \
            .option("sfWarehouse", snowflake_config['sf_warehouse']) \
            .option("sfRole", snowflake_config['sf_role']) \
            .option("sfSchema", schema_name) \
            .option("sfUser", snowflake_config['sf_user']) \
            .option("sfPassword", snowflake_config['sf_password']) \
            .option("dbtable", table_name) \
            .mode("append") \
            .save()