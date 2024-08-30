import datetime

def date_to_epoch(target_date):
    """
    Convert a target date to an Ethereum epoch.

    Parameters:
    start_epoch (int): The starting epoch.
    start_date (datetime.datetime): The date corresponding to the starting epoch.
    target_date (datetime.datetime): The date to convert to an epoch.

    Returns:
    int: The epoch corresponding to the target date.
    """
    
    start_epoch = 0
    start_date = datetime.datetime(2020, 12, 1, 12, 0, 23, tzinfo=datetime.timezone.utc)

    # Ensure target_date is a proper datetime object with UTC timezone
    if not isinstance(target_date, datetime.datetime):
        raise ValueError("target_date must be a datetime object")
    
    if target_date.tzinfo is None:
        raise ValueError("target_date must have a timezone (preferably UTC)")
    
    # Convert to UTC if it's not already
    target_date = target_date.astimezone(datetime.timezone.utc)

    # Calculate the difference in seconds between the target date and the start date
    time_difference = (target_date - start_date).total_seconds()
    
    # Calculate the number of epochs that have passed since the start date
    epochs_passed = time_difference // 384
    
    # Calculate the target epoch
    target_epoch = start_epoch + int(epochs_passed)
    
    return target_epoch


if __name__ == "__main__":
   
    # Test cases
    test_cases = [
        datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 6, 15, 12, 30, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2023, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    ]

    for test_date in test_cases:
        result_epoch = date_to_epoch(test_date)
        print(f"Date: {test_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"Corresponding Epoch: {result_epoch}")
        print()

