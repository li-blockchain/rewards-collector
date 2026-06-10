# discord_bot.py
import discord
import os
from ai.ai_handler import handle_ai_query
from dotenv import load_dotenv
from commands.earnings import generate_earnings_report
from commands.date_to_epoch import date_to_epoch
from commands.rocketpool_cycles import get_rocketpool_cycle
from commands.cdp import generate_cdp_report
import datetime
from pathlib import Path
from generate_invoice import InvoiceGenerator

# Load environment variables from .env file
load_dotenv()

intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f'We have logged in as {client.user}')

@client.event
async def on_message(message):
    print(message.content)

    parquet_file = 'rewards_data/rewards_master.parquet'

    if message.author == client.user:
        return
    
    if message.content.startswith('!help'):
        await message.channel.send(
            "👋 Hey, I'm Clark — I track LIBC validator rewards and watch our CDP position. I work nights.\n\n"
            "**📊 Reward reports** (by epoch)\n"
            "`!earnings <fromEpoch> <toEpoch>` — Summary: proposals, withdrawals, principal returned from exits, plus a per-node breakdown\n"
            "`!invoice <fromEpoch> <toEpoch>` — Generate a full Excel invoice for the range and post it here\n\n"
            "**🏦 CDP**\n"
            "`!cdp` — Current CDP position health (collateralization ratio & health factor)\n\n"
            "**💬 Just ask — `!yo <question>`** (plain English)\n"
            "`!yo rewards from 05/01/2025 to 05/18/2025` — Earnings by date (I convert dates → epochs)\n"
            "`!yo what Rocket Pool cycle are we in?` — Current cycle number + date range\n"
            "`!yo which cycle was Dec 11 2024?` — Cycle for any past date\n\n"
            "`!help` — Show this message"
        )

    if message.content.startswith('!earnings'):
        try:
            # Split the message content to get the command and parameters
            parts = message.content.split()
            if len(parts) != 3:
                await message.channel.send("Please provide both fromEpoch and toEpoch. Usage: !earnings <fromEpoch> <toEpoch>")
                return

            fromEpoch = int(parts[1])
            toEpoch = int(parts[2])

            await message.channel.send(f"Gotcha. Aggregating some earnings data from epoch {fromEpoch} to epoch {toEpoch}... hold please.")


            print(f"Running rewards aggregator from epoch {fromEpoch} to epoch {toEpoch} from parquet file {parquet_file}")

            response = generate_earnings_report(fromEpoch, toEpoch, parquet_file)

            await message.channel.send(response)
        except ValueError:
            await message.channel.send("Invalid block numbers. Please ensure fromEpoch and toEpoch are valid integers.")
        except Exception as e:
            await message.channel.send(f"An error occurred: {str(e)}")

    if message.content.startswith('!invoice'):
        try:
            # Split the message content to get the command and parameters
            parts = message.content.split()
            if len(parts) != 3:
                await message.channel.send("Please provide both fromEpoch and toEpoch. Usage: !invoice <fromEpoch> <toEpoch>")
                return

            fromEpoch = int(parts[1])
            toEpoch = int(parts[2])

            await message.channel.send(f"📊 Generating professional invoice for epochs {fromEpoch} to {toEpoch}... this may take a moment.")

            # Generate the invoice
            output_file = f"invoices/invoice_epochs_{fromEpoch}_{toEpoch}.xlsx"
            generator = InvoiceGenerator(parquet_file)
            generator.create_professional_invoice(
                output_file,
                fromEpoch,
                toEpoch,
                client_name="Valued Client",
                invoice_number=None
            )

            # Upload the file to Discord
            invoice_path = Path(output_file)
            if invoice_path.exists():
                await message.channel.send(
                    f"✅ Invoice generated successfully!",
                    file=discord.File(invoice_path)
                )
            else:
                await message.channel.send("❌ Failed to generate invoice file.")

        except ValueError:
            await message.channel.send("Invalid epoch numbers. Please ensure fromEpoch and toEpoch are valid integers.")
        except Exception as e:
            await message.channel.send(f"An error occurred while generating invoice: {str(e)}")

    if message.content.startswith('!cdp'):
        try:
            await message.channel.send("Checking CDP position status...")
            response = generate_cdp_report()
            await message.channel.send(response)
        except Exception as e:
            await message.channel.send(f"An error occurred while checking CDP position: {str(e)}")

    if message.content.startswith('!yo'):
        query = message.content[3:]  # Remove the '!ai' prefix
        response = await handle_ai_query(query)

        if response.startswith('!cycles'):
            try:
                # Split the message content to get the command and parameters
                parts = response.split()
                # await message.channel.send(f"parts: {parts}")
                if len(parts) == 1:
                    # No date given return a date in the middle of the current month.
                    # If we are on the last week then increment the month by one.
                    today = datetime.datetime.now()
                    # # Check if we're in the last week of the month
                    # if today.day > 21:
                    #     print(today.day)
                    #     # Move to the middle of next month
                    #     next_month = today.replace(day=1) + datetime.timedelta(days=32)
                    #     date_obj = next_month.replace(day=15)
                    # else:
                    #     # Use the middle of the current month
                    #     date_obj = today.replace(day=15)
                    date_obj = today
                    
                    cycle_info = get_rocketpool_cycle(date_obj.date())
                    await message.channel.send(f"Current cycle: Cycle {cycle_info['cycle_number']}, From: {cycle_info['from_date']}, To: {cycle_info['to_date']}")
                    return
                
                date = parts[1]
                
                cycle_info = get_rocketpool_cycle(date)
                await message.channel.send(f"Cycle: {cycle_info['cycle_number']}, From: {cycle_info['from_date']}, To: {cycle_info['to_date']}")
                return
            except ValueError as e:
                await message.channel.send("Invalid date format. Please ensure the date is in mm/dd/yyyy format.")
            except Exception as e:
                await message.channel.send(f"An error occurred: {str(e)}")

        # Check if the response contains an earnings command
        if response.startswith('!earnings'):
            try:
                # Extract dates from the response
                parts = response.split()
                if len(parts) != 3:
                    await message.channel.send("Invalid date format detected. Please provide both from and to dates.")
                    return
                from_date = parts[1]
                to_date = parts[2]

                # Convert date strings to datetime objects in UTC
                from_datetime = datetime.datetime.strptime(from_date, "%m/%d/%Y").replace(tzinfo=datetime.timezone.utc)
                # Use end of day (23:59:59) for the 'To' date to include all epochs from that day
                to_datetime = datetime.datetime.strptime(to_date, "%m/%d/%Y").replace(
                    hour=23, minute=59, second=59, tzinfo=datetime.timezone.utc
                )

                # Convert datetime objects to Unix timestamps
                from_timestamp = int(from_datetime.timestamp())
                to_timestamp = int(to_datetime.timestamp())


                print(f"From timestamp: {from_timestamp}, To timestamp: {to_timestamp}")
                await message.channel.send(f"From timestamp: {from_timestamp}, To timestamp: {to_timestamp}")
                # Convert dates to epochs
                from_epoch = date_to_epoch(from_datetime)
                to_epoch = date_to_epoch(to_datetime)

                await message.channel.send(f"Boom. Aggregating some earnings data from epoch {from_epoch} to epoch {to_epoch}... hold please.")

                # Generate and send the earnings report
                earnings_response = generate_earnings_report(from_epoch, to_epoch, parquet_file)
                await message.channel.send(earnings_response)
                return  # Exit the function after handling the earnings command

            except Exception as e:
                await message.channel.send(f"An error occurred while processing the earnings command: {str(e)}")
                return  # Exit the function after handling the error

        # If it's not an earnings command, send the original AI response
        await message.channel.send(response)

# Replace 'YOUR_DISCORD_BOT_TOKEN' with your actual bot token
client.run(os.getenv("DISCORD_BOT_TOKEN"))
