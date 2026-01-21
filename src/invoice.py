# rewards_aggregator.py
import pandas as pd
import json
from pathlib import Path
from reward_utils import adjust_reward

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

    # Process withdrawals and track exits
    for _, row in withdrawals_df.iterrows():
        withdrawal_record = {
            'amount': row['amount'],
            'epoch': row['epoch'],
            'node': row['node'],
            'type': row['validator_type']
        }

        # Handle exits (withdrawals > 32 ETH)
        if row['amount'] > 32 * 10**9:  # 32 ETH in gwei
            # Create exit record
            exit_record = withdrawal_record.copy()
            exit_record['amount'] = 32 * 10**9
            exits.append(exit_record)

            # Adjust withdrawal amount
            withdrawal_record['amount'] = row['amount'] - (32 * 10**9)
            withdrawals.append(withdrawal_record)
        else:
            withdrawals.append(withdrawal_record)

    return proposals, withdrawals, exits

def convert_wei_to_eth(wei):
    return wei / 10**18

def convert_gwei_to_eth(gwei):
    return gwei / 10**9

def aggregate_data(proposals, withdrawals):
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

    # Aggregate by node only if dataframes have data
    if not proposals_df.empty and 'node' in proposals_df.columns:
        proposals_summary = proposals_df.groupby(['node']).agg({'amount': 'sum'}).reset_index()
    else:
        proposals_summary = pd.DataFrame(columns=['node', 'amount'])
    
    if not withdrawals_df.empty and 'node' in withdrawals_df.columns:
        withdrawals_summary = withdrawals_df.groupby(['node']).agg({'amount': 'sum'}).reset_index()
    else:
        withdrawals_summary = pd.DataFrame(columns=['node', 'amount'])

    grand_total = total_proposals + total_withdrawals

    # Merge summaries safely
    combined_summary = pd.merge(proposals_summary, withdrawals_summary, on='node', how='outer').fillna(0)
    combined_summary.rename(columns={'amount_x': 'total_proposals', 'amount_y': 'total_withdrawals'}, inplace=True)

    return combined_summary, total_proposals, total_withdrawals, grand_total

def run_aggregator(fromBlock, toBlock, parquet_file='rewards_data/rewards_master.parquet'):
    proposals, withdrawals, exits = fetch_data(fromBlock, toBlock, parquet_file)

    print(f"proposals: {len(proposals)}, withdrawals: {len(withdrawals)}, exits: {len(exits)}")


    combined_summary, total_proposals, total_withdrawals, grand_total = aggregate_data(proposals, withdrawals)
    
    result = {
        "combined_summary": combined_summary.to_dict(orient='records'),
        "total_proposals": total_proposals,
        "total_withdrawals": total_withdrawals,
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

    # print("\nExits Summary:")
    # for record in result["combined_summary"]:
    #     print(f"Node: {record['node']}, Total Exits: {record['total_exits']}")

    print("\nTotals Summary:")
    print(f"Total Proposals: {result['total_proposals']}")
    print(f"Total Withdrawals: {result['total_withdrawals']}")
    print(f"Grand Total: {result['grand_total']}")
