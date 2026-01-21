"""
This module provides functionality to calculate Rocket Pool cycles based on input dates.

Rocket Pool operates on 28-day cycles, starting from its launch date on Sept 1, 2022.
This script includes a function to determine the cycle number and date range for any given date.

Key Features:
- Calculates the Rocket Pool cycle number for a given date
- Determines the start and end dates of the cycle containing the input date
- Useful for tracking rewards, analyzing performance, or scheduling operations in sync with Rocket Pool cycles

Usage:
    from rocketpool_cycles import get_rocketpool_cycle
    result = get_rocketpool_cycle(datetime.date(2023, 5, 15))
    print(f"Cycle: {result['cycle_number']}, From: {result['from_date']}, To: {result['to_date']}")
"""
import datetime

def get_rocketpool_cycle(input_date):
    # Define the start date and initial cycle number
    start_date = datetime.date(2022, 9, 1)  # Rocketpool launch date
    cycle_length = datetime.timedelta(days=28)

    if not isinstance(input_date, datetime.date):
        try:
            input_date = datetime.datetime.strptime(input_date, "%m/%d/%Y").date()
        except ValueError:
            raise ValueError("input_date must be a datetime.date instance or a string in the format mm/dd/yyyy")

    # Calculate the number of cycles since the start date
    days_since_start = (input_date - start_date).days
    cycle_number = (days_since_start // 28) + 1
    
    # Calculate the start and end dates of the current cycle
    cycle_start = start_date + (cycle_number - 1) * cycle_length
    cycle_end = cycle_start + cycle_length - datetime.timedelta(days=1)

    # Convert the cycle_start and cycle_end dates to the format mm/dd/yyyy
    cycle_start_formatted = cycle_start.strftime("%m/%d/%Y")
    cycle_end_formatted = cycle_end.strftime("%m/%d/%Y")
    
    return {
        "cycle_number": cycle_number,
        "from_date": cycle_start_formatted,
        "to_date": cycle_end_formatted
    }

if __name__ == "__main__":
    # Test the function with a sample date
    test_date = datetime.date(2024, 9, 15)
    result = get_rocketpool_cycle(test_date)
    
    print(f"Input date: {test_date}")
    print(f"Cycle number: {result['cycle_number']}")
    print(f"Cycle start date: {result['from_date']}")
    print(f"Cycle end date: {result['to_date']}")
