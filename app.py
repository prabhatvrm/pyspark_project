from util import get_spark_session
import os
import read
import process
import write
import validate

def main():
    env = os.environ.get('ENV')
    src_path = os.environ.get('SRC_PATH')
    src_file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    tgt_path = os.environ.get("TGT_PATH")
    spark = get_spark_session(env)
    print(spark.sparkContext.appName)
    gh_read_df=read.gh_read(spark,src_path,src_file_pattern)
    gh_transform = gh_read_df.select("repo","created_at")
    gh_transform_df = process.gh_process(gh_transform)
    write.gh_write(gh_transform_df,tgt_path)
    validate.validate_count(gh_read_df,gh_transform_df)


if __name__ == '__main__':
    main()
