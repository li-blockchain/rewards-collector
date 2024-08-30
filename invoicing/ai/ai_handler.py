import openai
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set your OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

# Load system prompt from a file
with open("system_prompt.txt", "r") as file:
    system_prompt = file.read().strip()

async def handle_ai_query(query):
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ],
            max_tokens=100,
            temperature=0.5
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"An error occurred while processing the query: {str(e)}"


if __name__ == "__main__":
    import asyncio

    async def main():
        test_query = "What is the capital of France?"
        result = await handle_ai_query(test_query)
        print(f"Query: {test_query}")
        print(f"Response: {result}")

    asyncio.run(main())



