import pandas as pd
import time

file_path = "/home/hadoop/US_Accidents_March23.csv"

row_sizes = [
    100000,
    500000,
    1000000,
    3000000,
    None
]

for nrows in row_sizes:

    if nrows is None:
        label = "Full Dataset"
    else:
        label = f"{nrows:,} rows"

    print("\n" + "=" * 50)
    print(label)
    print("=" * 50)

    start_time = time.time()

    df = pd.read_csv(
        file_path,
        usecols=["State"],
        nrows=nrows
    )

    result = (
        df.dropna(subset=["State"])
          .groupby("State")
          .size()
          .sort_values(ascending=False)
          .head(10)
    )

    end_time = time.time()

    print(result)

    print("\nRows processed:", len(df))
    print(
        "Execution time:",
        round(end_time - start_time, 3),
        "seconds"
    )

    print(
        "Memory used:",
        round(
            df.memory_usage(deep=True).sum()
            / 1024**3,
            3
        ),
        "GB"
    )

    del df
    del result