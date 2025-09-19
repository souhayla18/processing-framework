def write_data(df, path):
    df.write.mode("overwrite").json(path)
