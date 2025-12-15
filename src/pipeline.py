import numpy as np
import pandas as pd
import json
from dateutil import parser
import re
import os

pd.set_option("display.max_columns", None)



def is_not_null(item):
    if item:
        if str(item).strip().lower() not in ['nan', 'null', 'none', '']:
            return True
    return False


def normalize_str(in_str: str) -> str:
    if is_not_null(in_str):
        return in_str.strip()
    return np.nan


def normalize_email(in_email: str) -> str:
    if is_not_null(in_email):
        return in_email.strip().lower()
    return np.nan


def normalize_date(in_date: str) -> str:
    if is_not_null(in_date):
        return parser.parse(in_date).strftime("%Y-%m-%d")
    return np.nan


def normalize_phone(in_phone: str) -> str:
    output = np.nan
    if is_not_null(in_phone):
        digits = re.sub(r"\D", "", in_phone)
        if len(digits) != 10:
            print(f'Not standard phone number: {in_phone}')
        else:
            output = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return output


def normalize_value(in_type: str, in_value) -> str:
    if in_type == 'string':
        return normalize_str(in_value)
    elif in_type == 'email':
        return normalize_email(in_value)
    elif in_type == 'date':
        return normalize_date(in_value)
    elif in_type == 'phone':
        return normalize_phone(in_value)
    return in_value


def normalize_files(in_dir: str, schema_mapping: str):

    df_list = []
    with open(schema_mapping, 'r') as f:
        schema = json.load(f)
    schema_columns = schema['columns']

    for file in os.listdir(in_dir):
        df = pd.read_csv(os.path.join(in_dir, file))
        df_columns = df.columns.values

        new_columns = []
        for fld in df_columns:
            new_columns.append(fld)
            for sch_fld in schema_columns:
                if fld in sch_fld['values']:
                    new_columns.pop()
                    new_columns.append(sch_fld['name'])
                    df[fld] = df[fld].apply(lambda x: normalize_value(sch_fld['type'], x))
                    break
        df.columns = new_columns
        df_list.append(df)
        print(df)
        print('-' * 100)

    return df_list


def merge_dataFrames(df_list, out_parquet):
    res_df = df_list[0]
    for i in range(0, len(df_list)):
        if i < len(df_list) - 1:
            if 'created_date' in res_df and 'created_date' in df_list[i+1]:
                res_df = pd.merge(res_df, df_list[i+1], on=['name', 'created_date'], how='outer')
            else:
                res_df = pd.merge(res_df, df_list[i + 1], on=['name'], how='outer')
    print(res_df)

    # res_df.to_parquet(out_parquet, engine='pyarrow', index=False)


df_list = normalize_files(r'../data_raw', r'../config/schema_mapping.json')
merge_dataFrames(df_list, r'../data_processed/output.parquet')