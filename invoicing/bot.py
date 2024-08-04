# discord_bot.py
import discord
import os
from invoice import run_aggregator

intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f'We have logged in as {client.user}')

@client.event
async def on_message(message):
    print(message.content)
    if message.author == client.user:
        return
    
    if message.content.startswith('!help'):
        await message.channel.send("Hi! I'm Clark. I can help you aggregate earnings data. I work nights. Usage: !earnings <fromEpoch> <toEpoch>")

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
            collection_name = 'rewards_v2'

            print(f"Running rewards aggregator from epoch {fromEpoch} to epoch {toEpoch} for collection {collection_name}")

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
                response += f"Node: {record['node']}, Total Proposals: {record['total_proposals']}, Total Withdrawals: {record['total_withdrawals']}\n"

            await message.channel.send(response)
        except ValueError:
            await message.channel.send("Invalid block numbers. Please ensure fromEpoch and toEpoch are valid integers.")
        except Exception as e:
            await message.channel.send(f"An error occurred: {str(e)}")

# Replace 'YOUR_DISCORD_BOT_TOKEN' with your actual bot token
client.run('YOUR_DISCORD_BOT_TOKEN')
