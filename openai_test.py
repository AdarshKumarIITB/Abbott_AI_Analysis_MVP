import os
from dotenv import load_dotenv
from openai import OpenAI  # new client interface

load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("❌ OPENAI_API_KEY not found in .env")
    exit(1)

client = OpenAI(api_key=api_key)

try:
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": "Say hello from my CLI!"}]
    )
    print("✅ OpenAI response:", response.choices[0].message.content)
except Exception as e:
    print("❌ OpenAI API call failed:", e)
