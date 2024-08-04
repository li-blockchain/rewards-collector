# rewards_aggregator.py
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import json

# Initialize Firebase
cred = credentials.Certificate('../serviceAccountKey.json')
firebase_admin.initialize_app(cred)

db = firestore.client()

def adjust_reward(amount, type):
    """
    Adjust the reward based on the type of validator.

    Parameters:
    amount (float): The amount to adjust.
    type (float): The type of validator.

    Returns:
    int: The adjusted reward.
    """
    try:
        type = int(float(type))
    except ValueError:
        return amount

    # LEB8
    if 8 <= type < 15:
        bonded = amount // 4
        borrowed = amount - bonded
        return int(bonded + borrowed * 0.14)

    # LEB16
    if 16 <= type < 17:
        bonded = amount // 2
        borrowed = amount - bonded
        return int(bonded + borrowed * 0.15)

    return amount

def calculate_document_name(epoch):
    return ((epoch + 99) // 100) * 100

def fetch_data(fromBlock, toBlock, collection_name):
    proposals = []
    withdrawals = []

    start_doc = calculate_document_name(fromBlock)
    end_doc = calculate_document_name(toBlock)

    for doc in range(start_doc, end_doc + 100, 100):
        doc_ref = db.collection(collection_name).document(str(doc))
        doc = doc_ref.get()
        if doc.exists:
            data = doc.to_dict()
            proposals.extend([proposal for proposal in data.get('proposals', []) if fromBlock <= proposal['epoch'] <= toBlock])
            withdrawals.extend([withdrawal for withdrawal in data.get('withdrawals', []) if fromBlock <= withdrawal['epoch'] <= toBlock])

    return proposals, withdrawals

def convert_wei_to_eth(wei):
    return wei / 10**18

def convert_gwei_to_eth(gwei):
    return gwei / 10**9

def aggregate_data(proposals, withdrawals):
    proposals_df = pd.DataFrame(proposals)
    withdrawals_df = pd.DataFrame(withdrawals)

    # Adjust rewards based on validator type
    proposals_df['amount'] = proposals_df.apply(lambda x: adjust_reward(x['amount'], x['type']), axis=1)
    withdrawals_df['amount'] = withdrawals_df.apply(lambda x: adjust_reward(x['amount'], x['type']), axis=1)

    # Convert amounts to ETH
    proposals_df['amount'] = proposals_df['amount'].apply(convert_wei_to_eth)
    withdrawals_df['amount'] = withdrawals_df['amount'].apply(convert_gwei_to_eth)

    # Aggregate by node
    proposals_summary = proposals_df.groupby(['node']).agg({'amount': 'sum'}).reset_index()
    withdrawals_summary = withdrawals_df.groupby(['node']).agg({'amount': 'sum'}).reset_index()

    total_proposals = proposals_df['amount'].sum()
    total_withdrawals = withdrawals_df['amount'].sum()
    grand_total = total_proposals + total_withdrawals

    combined_summary = pd.merge(proposals_summary, withdrawals_summary, on='node', how='outer').fillna(0)
    combined_summary.rename(columns={'amount_x': 'total_proposals', 'amount_y': 'total_withdrawals'}, inplace=True)

    return combined_summary, total_proposals, total_withdrawals, grand_total

def run_aggregator(fromBlock, toBlock, collection_name):
    proposals, withdrawals = fetch_data(fromBlock, toBlock, collection_name)

    combined_summary, total_proposals, total_withdrawals, grand_total = aggregate_data(proposals, withdrawals)
    
    result = {
        "combined_summary": combined_summary.to_dict(orient='records'),
        "total_proposals": total_proposals,
        "total_withdrawals": total_withdrawals,
        "grand_total": grand_total
    }
    
    return result

if __name__ == "__main__":
    fromBlock = 282497
    toBlock = 288684
    collection_name = 'rewards_v2'

    result = run_aggregator(fromBlock, toBlock, collection_name)

    print("Combined Summary:")
    print(result["combined_summary"])

    print("\nTotals Summary:")
    print(f"Total Proposals: {result['total_proposals']}")
    print(f"Total Withdrawals: {result['total_withdrawals']}")
    print(f"Grand Total: {result['grand_total']}")
