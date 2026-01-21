from invoice import run_aggregator


def generate_earnings_report(fromEpoch, toEpoch, parquet_file='rewards_data/rewards_master.parquet'):
    """Generate earnings report from parquet data."""
    # Run the rewards aggregator with parquet file
    result = run_aggregator(fromEpoch, toEpoch, parquet_file)

    # Create a response message
    combined_summary = result["combined_summary"]
    total_proposals = result["total_proposals"]
    total_withdrawals = result["total_withdrawals"]
    grand_total = result["grand_total"]

    response = "Earnings Summary:\n"
    response += f"Total Proposals: {total_proposals:.6f} ETH\n"
    response += f"Total Withdrawals: {total_withdrawals:.6f} ETH\n"
    response += f"Grand Total: {grand_total:.6f} ETH\n"
    response += "\nCombined Summary:\n"
    for record in combined_summary:
        response += f"Node: {record['node']}, Total Proposals: {record['total_proposals']:.6f} ETH, Total Withdrawals: {record['total_withdrawals']:.6f} ETH\n"

    return response
