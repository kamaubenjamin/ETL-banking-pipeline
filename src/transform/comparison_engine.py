import pandas as pd


def combine_datasets(datasets: dict) -> pd.DataFrame:
    """
    Combine multiple datasets into one with source labels

    datasets = {
        "jumia": df1,
        "kilimall": df2
    }
    """

    combined = []

    for source, df in datasets.items():
        temp = df.copy()
        temp["source"] = source
        combined.append(temp)

    return pd.concat(combined, ignore_index=True)