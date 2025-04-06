import json
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.json_util import dumps  # Import the dumps function
import re

# Load environment variables from .env file
load_dotenv()

# Fetch the OpenAI API key from environment variables
api_key = os.getenv("OPENAI_API_KEY")

if not api_key:
    raise ValueError("API key not found in environment variables. Please set it in the .env file.")

# Initialize LangChain model
model = ChatOpenAI(model="gpt-3.5-turbo", openai_api_key=api_key)

# System template for MongoDB query generation
system_template = """
You are an expert MongoDB query generator.

Your task is to:
1. Interpret the user's question.
2. Generate a valid MongoDB aggregation pipeline query in JSON format, directly usable in Python's `json.loads()` function, based on the question.
3. Ensure the output conforms to the schema below and accurately answers the question.

Schema for the database:
- `symbol`: Name of the company (e.g., "CIT").
- `buyer`: Broker buying the shares.
- `seller`: Broker selling the shares.
- `quantity`: Number of shares traded.
- `rate`: Price of a share.
- `amount`: Total amount (`rate * quantity`).
- `transaction_date`: Date of the trade.

Rules for query generation:
1. Always filter by `symbol` if specified in the user's question.
2. Support the following operations based on the user's question:
   - Aggregating data (e.g., by `buyer`, `seller`, `transaction_date`, or `symbol`).
   - Summing, averaging, or calculating totals (e.g., `total quantity`, `total amount`).
   - Filtering by specific fields (e.g., symbols, brokers, date ranges).
   - Performing calculations like **profit or loss**, **profit margin**, and **average rate**.
   - Sorting, finding top results (e.g., "top buyer", "broker with most shares"), or ranking data.
   - Combining data using `$lookup` to reference related documents, if needed.
   - Handling edge cases such as missing or null values gracefully.
   - Use appropriate date comparisons (`$gte`, `$lte`, `$year`, etc.) when working with dates.
3. Return additional computed fields if explicitly requested (e.g., "profit", "margin").
4. Prioritize sorting and limits for questions about "most", "top", or "highest".

5. When working with dates:
   - If the question specifies a year, extract it using the `$year` operator .
   - If the question specifies a date range, use `$match` with `$gte` and `$lte` for filtering .
   - Always ensure the `transaction_date` format is correctly handled to match the stored format.
   - Do not use ISODate 
   

Output format:
1. Return only the valid MongoDB aggregation pipeline as a JSON array.
2. Do not include explanations, justifications, or additional commentary.
3. Ensure the pipeline JSON is error-free and accounts for edge cases.

Example questions your system can handle:
1. "What is the total quantity of shares bought by each broker for CIT?"
2. "Which broker has the most shares in symbol 'CIT'?"
3. "Who bought the most shares of CIT in 2024?"
4. "What is the profit margin for all brokers trading CIT?"
5. "Who are the top 5 buyers of CIT based on total amount spent?"
6. "What is the total quantity traded for all companies on 2025-01-01?"
7. "Show the average rate and total quantity sold by each seller for CIT."
8. "What is the profit or loss for a specific broker trading CIT?"
9. "What is the average rate and total amount for transactions in 2024?"
"""


# Create the prompt template
prompt_template = ChatPromptTemplate.from_messages(
    [("system", system_template), ("user", "{text}")]
)

# MongoDB connection
client = MongoClient("mongodb://tradems:Trade%40DB@128.199.29.8:27017/admin")
db = client["admin"]
collection = db["scraped_data"]

def preprocess_and_validate_ai_response(ai_response):
    try:
        preprocessed_response = ai_response.strip()
        print("Raw Response:", preprocessed_response)

        if preprocessed_response.startswith("{"):
            #replace newline characters outside curly braces
            cleaned_response = re.sub(r"}\s*\n\s*{", "},{", preprocessed_response)
            print("Cleaned Response:", cleaned_response) # Print the result of the cleaning

            #enclose in array brackets
            json_string = "[" + cleaned_response + "]"


            try:
                pipeline = json.loads(json_string)
            except json.JSONDecodeError as e:
                print(f"String that caused the error: {json_string}")
                raise ValueError(f"AI response is still not valid JSON after cleaning: {e}")  


        elif preprocessed_response.startswith("["):
            pipeline = json.loads(preprocessed_response) 

        else:
            raise ValueError("AI response is not a valid JSON object or array")
            

        if isinstance(pipeline, dict):
            pipeline = [pipeline]

        valid_pipeline = []
        if len(pipeline) == 1 and isinstance(pipeline[0], dict):
            for key, value in pipeline[0].items():
                valid_pipeline.append({key: value})
        elif isinstance(pipeline, list):
            for stage in pipeline:
                if not isinstance(stage, dict) or len(stage) != 1:
                    raise ValueError(f"Invalid pipeline stage: {stage}")
                valid_pipeline.append(stage)
        return valid_pipeline

    except json.JSONDecodeError as e:
        raise ValueError(f"AI response is not valid JSON: {e}")

    except Exception as e:
        raise ValueError(f"Error processing AI response: {e}")
    
    # try:
    #     preprocessed_response = ai_response.strip()
    #     print("Raw AI Response:", preprocessed_response)  # Print the raw response!
    #     pipeline = json.loads(preprocessed_response)
        
    #     if isinstance(pipeline, dict):
    #         pipeline = [pipeline]

    #     valid_pipeline = []
    #     if len(pipeline) == 1 and isinstance(pipeline[0], dict):
    #         for key, value in pipeline[0].items():
    #             valid_pipeline.append({key: value})
    #     elif isinstance(pipeline, list):
    #         for stage in pipeline:
    #             if not isinstance(stage, dict) or len(stage) != 1:
    #                 raise ValueError(f"Invalid pipeline stage: {stage}")
    #             valid_pipeline.append(stage)
    #     return valid_pipeline
    # except json.JSONDecodeError as e:
    #     raise ValueError(f"AI response is not valid JSON: {e}")
    # except Exception as e:
    #     raise ValueError(f"Error processing AI response: {e}")

def generate_mongodb_query(question):
    prompt = prompt_template.format(text=question)
    response = model.invoke(prompt)
    ai_response = response.content.strip()
    return preprocess_and_validate_ai_response(ai_response)

def execute_query(pipeline):
    try:
        results = list(collection.aggregate(pipeline))
        # Directly convert with bson.json_util.dumps
        return dumps(results, indent=2) 
    except Exception as e:
        raise ValueError(f"Error executing MongoDB query: {e}")

def main():
    while True:
        try:
            question = input("Enter your question (or type 'q' to quit): ")
            if question.lower() == "q":
                break

            pipeline = generate_mongodb_query(question)
            print("Generated Pipeline:", json.dumps(pipeline, indent=2))  # Print for debugging

            results = execute_query(pipeline)
            print("Results:", results)

        except ValueError as e:
            print(f"Error: {e}")
        except KeyboardInterrupt:  # Allow Ctrl+C to exit gracefully
            print("\nExiting...")
            break


if __name__ == "__main__":
    main()


