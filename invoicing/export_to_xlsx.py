#!/usr/bin/env python3
"""
Parquet to Excel Exporter

Exports rewards_master.parquet to Excel format for easy viewing and analysis.
"""

import pandas as pd
import argparse
from pathlib import Path


def export_parquet_to_xlsx(parquet_file, output_file=None, epoch_range=None):
    """Export parquet file to Excel format."""

    # Check if parquet file exists
    if not Path(parquet_file).exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_file}")

    # Load parquet file
    print(f"Loading data from {parquet_file}...")
    df = pd.read_parquet(parquet_file)

    # Filter by epoch range if provided
    if epoch_range:
        start_epoch, end_epoch = epoch_range
        print(f"Filtering epochs {start_epoch} to {end_epoch}...")
        df = df[(df['epoch'] >= start_epoch) & (df['epoch'] <= end_epoch)]

    # Generate output filename if not provided
    if not output_file:
        parquet_path = Path(parquet_file)
        output_file = parquet_path.parent / f"{parquet_path.stem}.xlsx"

    # Convert datetime column to proper Excel format if it exists
    if 'datetime' in df.columns and df['datetime'].notna().any():
        df['datetime'] = pd.to_datetime(df['datetime'], unit='s', errors='coerce')

    # Create Excel writer with multiple sheets
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # All data sheet
        df.to_excel(writer, sheet_name='All Records', index=False)

        # Withdrawals only
        withdrawals_df = df[df['record_type'] == 'withdrawal']
        if not withdrawals_df.empty:
            withdrawals_df.to_excel(writer, sheet_name='Withdrawals', index=False)

        # Proposals only
        proposals_df = df[df['record_type'] == 'proposal']
        if not proposals_df.empty:
            proposals_df.to_excel(writer, sheet_name='Proposals', index=False)

        # Summary by node
        if not df.empty:
            # Convert amounts to ETH for summary
            df_summary = df.copy()

            # Convert gwei to ETH for withdrawals, wei to ETH for proposals
            df_summary.loc[df_summary['record_type'] == 'withdrawal', 'amount_eth'] = df_summary['amount'] / 10**9
            df_summary.loc[df_summary['record_type'] == 'proposal', 'amount_eth'] = df_summary['amount'] / 10**18

            summary = df_summary.groupby(['node', 'record_type']).agg({
                'amount_eth': 'sum',
                'validator_index': 'count'
            }).reset_index()
            summary.rename(columns={'validator_index': 'count'}, inplace=True)
            summary.to_excel(writer, sheet_name='Summary by Node', index=False)

    print(f"✅ Exported {len(df)} records to {output_file}")
    print(f"   📥 Withdrawals: {len(withdrawals_df)}")
    print(f"   📤 Proposals: {len(proposals_df)}")

    if epoch_range:
        print(f"   📅 Epoch range: {start_epoch} - {end_epoch}")
    else:
        epoch_min = df['epoch'].min() if not df.empty else 0
        epoch_max = df['epoch'].max() if not df.empty else 0
        print(f"   📅 Epoch range: {epoch_min} - {epoch_max}")


def main():
    parser = argparse.ArgumentParser(description='Export parquet file to Excel')
    parser.add_argument('--input', default='rewards_data/rewards_master.parquet',
                       help='Input parquet file path')
    parser.add_argument('--output', help='Output Excel file path')
    parser.add_argument('--start-epoch', type=int, help='Start epoch for filtering')
    parser.add_argument('--end-epoch', type=int, help='End epoch for filtering')

    args = parser.parse_args()

    # Validate epoch range
    epoch_range = None
    if args.start_epoch is not None and args.end_epoch is not None:
        if args.start_epoch > args.end_epoch:
            print("Error: start-epoch must be <= end-epoch")
            return
        epoch_range = (args.start_epoch, args.end_epoch)
    elif args.start_epoch is not None or args.end_epoch is not None:
        print("Error: Both --start-epoch and --end-epoch must be provided together")
        return

    try:
        export_parquet_to_xlsx(args.input, args.output, epoch_range)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()