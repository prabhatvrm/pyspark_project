def gh_read(spark, src_path, src_file_pattern):
    gh_read_df = spark.read.json(f"{src_path}\\{src_file_pattern}")
    return gh_read_df
