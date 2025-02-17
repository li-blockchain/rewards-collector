from invoice import run_aggregator


def generate_earnings_report(fromEpoch, toEpoch, collection_name):
    # Run the rewards aggregator
    result = run_aggregator(fromEpoch, toEpoch, collection_name)

    # Create a response message
    combined_summary = result["combined_summary"]
    total_proposals = result["total_proposals"]
    total_withdrawals = result["total_withdrawals"]
    grand_total = result["grand_total"]

    response = "Earnings Summary:\n"
    response += f"Total Proposals: {total_proposals}\n"
    response += f"Total Withdrawals: {total_withdrawals}\n"
    response += f"Grand Total: {grand_total}\n"
    response += "\nCombined Summary:\n"
    for record in combined_summary:
        response += f"Node: {record['node']}, Total Proposals: {record['total_proposals']}, Total Withdrawals: {record['total_withdrawals']}"
        if record['total_exits'] > 0:
            response += f", Total Exits: {record['total_exits']}"
        response += "\n"
    
    return response
