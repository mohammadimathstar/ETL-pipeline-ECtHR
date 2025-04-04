import pandas as pd
import unicodedata


# Function to remove accents
def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return nfkd_form.encode('ASCII', 'ignore').decode('utf-8')


def transform_data(local_file_path):

    df = pd.read_csv(local_file_path)
    
    df['kpdate'] = pd.to_datetime(df['kpdate'])
    df['originatingbody'] = df['originatingbody'].astype('Int64')
    df['typedescription'] = df['typedescription'].astype('Int64')

    # Define fill values based on data type
    fill_values = {
        'object': 'None',   # For text columns
        'float64': -1.0,       # For float columns
        'int64': -1,            # For integer columns
        'Int64': -1,
    }

    # Iterate over each column and fill missing values based on data type
    for col in df.columns:
        col_type = df[col].dtype
        if col_type == 'object':
            df[col].fillna(fill_values['object'], inplace=True)
        elif col_type == 'float64':
            df[col].fillna(fill_values['float64'], inplace=True)
        elif col_type == 'int64':
            df[col].fillna(fill_values['int64'], inplace=True)
        elif col_type == 'Int64':
            df[col].fillna(fill_values['Int64'], inplace=True)

    df['originatingbody'] = df['originatingbody'].astype('int64')
    df['typedescription'] = df['typedescription'].astype('int64')

    df.to_csv(local_file_path, index=False)
