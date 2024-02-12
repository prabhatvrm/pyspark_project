def gh_write(df,tgt_path):
    df.coalesce(16). \
        write. \
        partitionBy('year', 'month', 'day'). \
        mode('append'). \
        format("json"). \
        save(tgt_path)