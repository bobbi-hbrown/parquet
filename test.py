import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
import os
import re

directory_in_str = os.path.dirname(os.path.realpath(__file__))

directory = os.fsencode(directory_in_str)

pattern = '([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) \\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\" (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\") ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$'

for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith(".txt"): 

        headers = ["Bucket Owner", "Bucket", "Time", "Remote IP", "Requester", "Request ID", "Operation", "Key", "Request-URI", "HTTP status", "Error Code", "Bytes Sent", "Object Size", "Total Time", "Turn-Around Time", "Referrer", "User-Agent", "Version Id", "Host Id", "Signature Version", "Cipher Suite", "Authentication Type", "Host Header", "TLS version"]
        data = []
        f = open(file, "r").read().split(" ")

        for line in f:
            match = re.search(pattern, line)
            if match:
                # Make sure to add \n to display correctly when we write it back
                new_line = match.group() + '\n'
                data.append(new_line)

            #print( dict(zip(headers, data)) )
            df = pd.DataFrame( dict(zip(headers, data)), index=[0] )
            table = pa.Table.from_pandas(df)

            pq.write_table(table, str(file).split(".")[0] + '.parquet')
            
            # print parquet file as dataframe in order to visualise data
            aws_data = pq.read_table(str(file).split(".")[0] + '.parquet')
            table = aws_data.to_pandas() 

            print(table)
            