"""
Shared utility functions for reward calculations.
"""

def adjust_reward(amount, validator_type):
    """
    Adjust the reward based on the type of validator.

    For LEB (Liquid Ethereum Bond) validators, only a portion of the reward
    is kept based on the commission structure:
    - LEB8 (8-14 ETH): 14% commission on borrowed portion
    - LEB16 (16 ETH): 15% commission on borrowed portion
    - Standard (32 ETH): 100% of rewards

    Parameters:
    amount (float): The amount to adjust.
    validator_type (float): The type of validator.

    Returns:
    int: The adjusted reward.
    """
    try:
        validator_type = int(float(validator_type))
    except (ValueError, TypeError):
        return amount

    # LEB8
    if 8 <= validator_type < 15:
        bonded = amount // 4
        borrowed = amount - bonded
        return int(bonded + borrowed * 0.14)

    # LEB16
    if 16 <= validator_type < 17:
        bonded = amount // 2
        borrowed = amount - bonded
        return int(bonded + borrowed * 0.15)

    return amount


def get_validator_type_label(validator_type):
    """
    Convert validator type to ETH amount label (8, 16, or 32 ETH).

    Parameters:
    validator_type: The validator type value

    Returns:
    str: Label like "8 ETH", "16 ETH", or "32 ETH"
    """
    if not validator_type:
        return '32 ETH'

    try:
        vtype = int(float(validator_type))
        if 8 <= vtype < 15:
            return '8 ETH'
        elif 16 <= vtype < 17:
            return '16 ETH'
        else:
            return '32 ETH'
    except (ValueError, TypeError):
        return '32 ETH'
