# Databricks notebook source
# MAGIC %pip install langchain
# MAGIC %pip install openai
# MAGIC %pip install mlflow

# COMMAND ----------

import os
# Azure OpenAI 
os.environ['OPENAI_API_TYPE'] = "azure"
os.environ['OPENAI_API_VERSION'] = "2023-08-01-preview"
os.environ['OPENAI_API_KEY'] = "4699cbb6884a438095ba926bc0a8e12d"
os.environ['OPENAI_API_BASE'] = "https://sandbox-ey.openai.azure.com/"
os.environ['DEPLOY_NAME'] = "gpt-4" # Equivalent to Model Name, but note missing '.' in 35
os.environ['OPENAI_API_VERSION'] = "2023-07-01-preview"

# COMMAND ----------

# Setup Chat context to use OpenAI via Azure
from langchain.chat_models import AzureChatOpenAI
from langchain.prompts.chat import ChatPromptTemplate
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler

llm = AzureChatOpenAI(
    openai_api_key=os.environ['OPENAI_API_KEY'],
    openai_api_base=os.environ['OPENAI_API_BASE'],
    openai_api_type=os.environ['OPENAI_API_TYPE'],
    openai_api_version=os.environ['OPENAI_API_VERSION'],
    deployment_name=os.environ['DEPLOY_NAME'],
    temperature=1.0,
    streaming=False,
    stop="\n"
)

# COMMAND ----------

# Generate prompt template

healthco_template = '''You are helpful AI assistant \n\nChat History

\n{history}

\n Human: {input}

\n AI:'''

prompt_template = ChatPromptTemplate.from_template(healthco_template)
print(prompt_template)

# COMMAND ----------

# Setup wrapper function around user session
from langchain.chains import ConversationChain
from langchain.chains import RetrievalQA
from langchain.memory import ConversationBufferMemory
from typing import List

sessions = {}

def get_response(model_input: List[str]) -> List[str]:
    
    session_id = model_input[0]
    message = model_input[1]
    
    if session_id not in sessions:

        # start a new session
        sessions[session_id] = ConversationChain(
            llm=llm,  # Use chat context generated above
            memory=ConversationBufferMemory(),  # Include memory to retain context. Should experiment with different types and retrieval mechanisms
            verbose=False,  # For PoC purposes to see reasoning.
            prompt=prompt_template  # Use Prompt template generated above.
        )
    conversation = sessions[session_id]
    return [conversation.run(message)]

# COMMAND ----------

get_response(['001', 'Hey how are you?'])

# COMMAND ----------

import mlflow

with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path = 'test_model', python_model = get_response, input_example = ['SID001', 'Hey there!'], registered_model_name = 'HHS-Test'
    )

loaded_model = mlflow.pyfunc.load_model(model_uri = model_info.model_uri)
