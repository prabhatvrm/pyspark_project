def validate_count(src_df,tgt_df):
    src_count=src_df.count()
    tgt_count=tgt_df.count()
    print(src_count)
    print(tgt_count)
    if src_count == tgt_count:
        print("No Data Lost")
    else:
        print("Data has Lost")
    pass