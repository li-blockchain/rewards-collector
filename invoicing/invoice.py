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
    exits = []

    start_doc = calculate_document_name(fromBlock)
    end_doc = calculate_document_name(toBlock)

    for doc in range(start_doc, end_doc + 100, 100):
        doc_ref = db.collection(collection_name).document(str(doc))
        doc = doc_ref.get()
        # Empty epoch 339400
        if doc.exists:
            data = doc.to_dict()
            if data:  # Check if data is not None
                # Safely get proposals and withdrawals with default empty lists
                doc_proposals = data.get('proposals', []) or []
                doc_withdrawals = data.get('withdrawals', []) or []
                
                # Only process if we have valid epoch data
                proposals.extend([
                    p for p in doc_proposals 
                    if p and 'epoch' in p and fromBlock <= p['epoch'] <= toBlock
                ])
                # Process withdrawals and track exits
                for w in doc_withdrawals:
                    if w and 'epoch' in w and fromBlock <= w['epoch'] <= toBlock:
                        if w['amount'] > 32 * 10**9:  # 32 ETH in gwei
                            # Create exit record with original withdrawal info
                            exit_record = w.copy()
                            exit_record['amount'] = 32 * 10**9
                            exits.append(exit_record)
                            
                            # Adjust withdrawal amount
                            w_adjusted = w.copy()
                            w_adjusted['amount'] = w['amount'] - (32 * 10**9)  # Subtract 32 ETH in gwei
                            withdrawals.append(w_adjusted)
                        else:
                            withdrawals.append(w)

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

def run_aggregator(fromBlock, toBlock, collection_name):
    proposals, withdrawals, exits = fetch_data(fromBlock, toBlock, collection_name)

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
    fromBlock = 370237 #262912 #338962 #282497
    toBlock = 376537#269212 #345037 #288684
    collection_name = 'rewards_v3'

    result = run_aggregator(fromBlock, toBlock, collection_name)

    print("Combined Summary:")
    print(result["combined_summary"])

    # print("\nExits Summary:")
    # for record in result["combined_summary"]:
    #     print(f"Node: {record['node']}, Total Exits: {record['total_exits']}")

    print("\nTotals Summary:")
    print(f"Total Proposals: {result['total_proposals']}")
    print(f"Total Withdrawals: {result['total_withdrawals']}")
    print(f"Grand Total: {result['grand_total']}")
