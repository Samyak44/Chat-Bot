from langchain_community.vectorstores import Pinecone
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.chains.question_answering import load_qa_chain
from langchain.schema import SystemMessage
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get API keys from environment variables
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
INDEX_NAME = "financial"  # Your existing index name

# Initialize OpenAI embeddings
embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)

# Initialize Pinecone and connect to existing index
vector_store = Pinecone.from_existing_index(
    index_name=INDEX_NAME,
    embedding=embeddings
)

# Initialize the language model
llm = ChatOpenAI(
    temperature=0,
    openai_api_key=OPENAI_API_KEY,
    model="gpt-3.5-turbo"
)

# Create the QA chain
chain = load_qa_chain(llm, chain_type="stuff")

# Main chat loop
print("Financial Document QA System")
print("Type 'exit' to quit")
print("-" * 50)

while True:
    # Get user question
    question = input("\nWhat would you like to know about the documents or write 'exit' to quite? "  ).strip()
    
    # Check for exit command
    if question.lower() == 'exit':
        print("\nGoodbye!")
        break
    
    if question:
        try:
            # Search for relevant documents
            docs = vector_store.similarity_search(question, k=3)
            
            # Generate answer
            answer = chain.run(input_documents=docs, question=question)
            
            print("\nAnswer:", answer)
            print("-" * 50)
            
        except Exception as e:
            print(f"\nError: {str(e)}")
            print("Please try again with a different question.")