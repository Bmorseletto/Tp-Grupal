import argparse
import os
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Generate M evenly sized datasets with N random entries from a CSV.")
    parser.add_argument("N", type=int, help="Number of random entries to grab")
    parser.add_argument("M", type=int, help="Number of datasets to generate")
    parser.add_argument("--source", type=str, default="datasets/LI-Small_Trans.csv", help="Source CSV file path")
    parser.add_argument("--output-dir", type=str, default=".", help="Directory to write output files")
    parser.add_argument("--prefix", type=str, default="input", help="Prefix for output files")
    args = parser.parse_args()

    df = pd.read_csv(args.source)
    if args.N > len(df):
        sample = df
    else:
        sample = df.sample(n=args.N, random_state=None)

    base_size = args.N // args.M
    remainder = args.N % args.M

    os.makedirs(args.output_dir, exist_ok=True)

    start = 0
    for i in range(args.M):
        size = base_size + (1 if i < remainder else 0)
        chunk = sample.iloc[start : start + size]
        start += size
        chunk.to_csv(os.path.join(args.output_dir, f"{args.prefix}_{i}.csv"), index=False)

    print(f"Generated {args.M} datasets in '{args.output_dir}' with a total of {args.N} entries.")


if __name__ == "__main__":
    main()
