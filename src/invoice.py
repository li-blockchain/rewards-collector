# rewards_aggregator.py
import pandas as pd
import json
from pathlib import Path
from reward_utils import adjust_reward, get_bonded_principal

def fetch_data(fromBlock, toBlock, parquet_file):
    """Fetch data from parquet file for given epoch range."""
    # Load parquet file
    if not Path(parquet_file).exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_file}")

    df = pd.read_parquet(parquet_file)

    # Filter by epoch range
    df_filtered = df[(df['epoch'] >= fromBlock) & (df['epoch'] <= toBlock)].copy()

    # Separate withdrawals and proposals
    withdrawals_df = df_filtered[df_filtered['record_type'] == 'withdrawal'].copy()
    proposals_df = df_filtered[df_filtered['record_type'] == 'proposal'].copy()

    # Convert to list of dicts for compatibility with existing code
    proposals = []
    withdrawals = []
    exits = []

    # Process proposals
    for _, row in proposals_df.iterrows():
        proposals.append({
            'amount': row['amount'],
            'epoch': row['epoch'],
            'node': row['node'],
            'type': row['validator_type']
        })

    # Check if is_exit column exists (new data format)
    has_exit_flag = 'is_exit' in withdrawals_df.columns
    # Minimum threshold to consider a withdrawal as an exit (8 ETH in gwei - minimum LEB bond)
    exit_threshold_gwei = 8 * 10**9

    # Process withdrawals and track exits
    for _, row in withdrawals_df.iterrows():
        withdrawal_record = {
            'amount': row['amount'],
            'epoch': row['epoch'],
            'node': row['node'],
            'type': row['validator_type']
        }

        # Determine if this is an exit withdrawal
        # Must have is_exit=True AND be above threshold (to filter out small skimmed rewards)
        if has_exit_flag:
            is_exit = row.get('is_exit', False) == True and row['amount'] >= exit_threshold_gwei
        else:
            # Fallback for old data without is_exit flag
            is_exit = row['amount'] > 32 * 10**9

        if is_exit:
            # For exits: principal (32 ETH) goes to exits list (no LEB adjustment)
            # Any excess goes to withdrawals list (will get LEB adjustment)
            principal_gwei = 32 * 10**9

            # Create exit record for the principal (no LEB adjustment applied)
            exit_record = withdrawal_record.copy()
            exit_record['amount'] = min(row['amount'], principal_gwei)
            exits.append(exit_record)

            # If there's any excess above 32 ETH, treat as regular withdrawal
            excess = row['amount'] - principal_gwei
            if excess > 0:
                withdrawal_excess = withdrawal_record.copy()
                withdrawal_excess['amount'] = excess
                withdrawals.append(withdrawal_excess)
        else:
            # Regular withdrawal (skimmed rewards) - normal LEB adjustment
            withdrawals.append(withdrawal_record)

    return proposals, withdrawals, exits

def convert_wei_to_eth(wei):
    return wei / 10**18

def convert_gwei_to_eth(gwei):
    return gwei / 10**9

def aggregate_data(proposals, withdrawals, exits=None):
    """Aggregate reward data by node.

    Args:
        proposals: List of proposal records (Wei amounts, LEB adjustment applied)
        withdrawals: List of withdrawal records (Gwei amounts, LEB adjustment applied)
        exits: List of exit records (Gwei amounts, NO LEB adjustment - principal return)
    """
    if exits is None:
        exits = []

    # Handle empty data frames
    if not proposals:
        proposals_df = pd.DataFrame(columns=['node', 'amount', 'type', 'epoch'])
        total_proposals = 0
    else:
        proposals_df = pd.DataFrame(proposals)
        # Adjust rewards based on validator type
        proposals_df['amount'] = proposals_df.apply(lambda x: adjust_reward(x['amount'], x['type']), axis=1)
        # Convert amounts to ETH
        proposals_df['amount'] = proposals_df['amount'].apply(convert_wei_to_eth)
        total_proposals = proposals_df['amount'].sum()

    if not withdrawals:
        withdrawals_df = pd.DataFrame(columns=['node', 'amount', 'type', 'epoch'])
        total_withdrawals = 0
    else:
        withdrawals_df = pd.DataFrame(withdrawals)
        # Adjust rewards based on validator type
        withdrawals_df['amount'] = withdrawals_df.apply(lambda x: adjust_reward(x['amount'], x['type']), axis=1)
        # Convert amounts to ETH
        withdrawals_df['amount'] = withdrawals_df['amount'].apply(convert_gwei_to_eth)
        total_withdrawals = withdrawals_df['amount'].sum()

    # Process exits - calculate bonded principal only (not borrowed portion)
    if not exits:
        exits_df = pd.DataFrame(columns=['node', 'amount', 'type', 'epoch'])
        total_exits = 0
    else:
        exits_df = pd.DataFrame(exits)
        # Apply bonded principal calculation (LEB8: 25%, LEB16: 50%, Standard: 100%)
        exits_df['amount'] = exits_df.apply(lambda x: get_bonded_principal(x['amount'], x['type']), axis=1)
        # Convert amounts to ETH
        exits_df['amount'] = exits_df['amount'].apply(convert_gwei_to_eth)
        total_exits = exits_df['amount'].sum()

    # Aggregate by node only if dataframes have data
    if not proposals_df.empty and 'node' in proposals_df.columns:
        proposals_summary = proposals_df.groupby(['node']).agg({'amount': 'sum'}).reset_index()
    else:
        proposals_summary = pd.DataFrame(columns=['node', 'amount'])

    if not withdrawals_df.empty and 'node' in withdrawals_df.columns:
        withdrawals_summary = withdrawals_df.groupby(['node']).agg({'amount': 'sum'}).reset_index()
    else:
        withdrawals_summary = pd.DataFrame(columns=['node', 'amount'])

    if not exits_df.empty and 'node' in exits_df.columns:
        exits_summary = exits_df.groupby(['node']).agg({'amount': 'sum'}).reset_index()
    else:
        exits_summary = pd.DataFrame(columns=['node', 'amount'])

    # Grand total excludes exits - exits are principal returns, not earnings
    grand_total = total_proposals + total_withdrawals

    # Merge summaries safely
    combined_summary = pd.merge(proposals_summary, withdrawals_summary, on='node', how='outer').fillna(0)
    combined_summary.rename(columns={'amount_x': 'total_proposals', 'amount_y': 'total_withdrawals'}, inplace=True)

    # Add exits to combined summary
    combined_summary = pd.merge(combined_summary, exits_summary, on='node', how='outer').fillna(0)
    combined_summary.rename(columns={'amount': 'total_exits'}, inplace=True)

    return combined_summary, total_proposals, total_withdrawals, total_exits, grand_total

def run_aggregator(fromBlock, toBlock, parquet_file='rewards_data/rewards_master.parquet'):
    proposals, withdrawals, exits = fetch_data(fromBlock, toBlock, parquet_file)

    print(f"proposals: {len(proposals)}, withdrawals: {len(withdrawals)}, exits: {len(exits)}")

    combined_summary, total_proposals, total_withdrawals, total_exits, grand_total = aggregate_data(
        proposals, withdrawals, exits
    )

    result = {
        "combined_summary": combined_summary.to_dict(orient='records'),
        "total_proposals": total_proposals,
        "total_withdrawals": total_withdrawals,
        "total_exits": total_exits,
        "grand_total": grand_total
    }

    return result

if __name__ == "__main__":
    fromBlock = 383059 #389362 #357862 #262912 #338962 #282497
    toBlock = 389306 #395437 #358537 #269212 #345037 #288684
    parquet_file = 'rewards_data/rewards_master.parquet'

    result = run_aggregator(fromBlock, toBlock, parquet_file)

    print("Combined Summary:")
    print(result["combined_summary"])

    print("\nTotals Summary:")
    print(f"Total Proposals: {result['total_proposals']}")
    print(f"Total Withdrawals: {result['total_withdrawals']}")
    print(f"Total Exits (principal returns): {result['total_exits']}")
    print(f"Grand Total: {result['grand_total']}")
