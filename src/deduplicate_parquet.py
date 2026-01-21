#!/usr/bin/env python3
"""
Deduplicate the rewards master parquet file.
"""
import pandas as pd
from pathlib import Path

def deduplicate_parquet(input_file='rewards_data/rewards_master.parquet',
                       output_file=None,
                       backup=True):
    """
    Remove duplicate records from parquet file.

    Duplicates are identified by:
    - epoch
    - validator_index
    - record_type
    - amount
    """
    input_path = Path(input_file)

    if not input_path.exists():
        print(f"❌ File not found: {input_file}")
        return

    # Backup original file
    if backup:
        backup_path = input_path.with_suffix('.parquet.backup')
        print(f"📦 Creating backup at {backup_path}")
        import shutil
        shutil.copy(input_path, backup_path)

    # Read parquet file
    print(f"📖 Reading {input_file}...")
    df = pd.read_parquet(input_path)
    original_count = len(df)
    print(f"   Original records: {original_count:,}")

    # Define key columns for identifying duplicates
    key_columns = ['epoch', 'validator_index', 'record_type', 'amount']

    # Find duplicates
    duplicates = df[df.duplicated(subset=key_columns, keep='first')]
    duplicate_count = len(duplicates)

    print(f"   Duplicate records: {duplicate_count:,}")

    if duplicate_count > 0:
        # Show some examples
        print("\n📋 Sample duplicates:")
        print(duplicates[key_columns].head(10))

        # Remove duplicates (keep first occurrence)
        df_clean = df.drop_duplicates(subset=key_columns, keep='first')
        final_count = len(df_clean)

        print(f"\n✨ After deduplication: {final_count:,} records")
        print(f"   Removed: {original_count - final_count:,} duplicates")

        # Save cleaned data
        output_path = Path(output_file) if output_file else input_path
        print(f"\n💾 Saving to {output_path}...")
        df_clean.to_parquet(output_path, index=False)

        print("✅ Deduplication complete!")
    else:
        print("✅ No duplicates found!")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Deduplicate rewards parquet file')
    parser.add_argument('--input', default='rewards_data/rewards_master.parquet',
                       help='Input parquet file')
    parser.add_argument('--output', help='Output parquet file (defaults to overwriting input)')
    parser.add_argument('--no-backup', action='store_true',
                       help='Skip creating backup file')

    args = parser.parse_args()

    deduplicate_parquet(
        input_file=args.input,
        output_file=args.output,
        backup=not args.no_backup
    )
