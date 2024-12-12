from typing import Any, Dict, List, Optional
import pandas as pd
import requests
import snowflake.connector
import streamlit as st

# Ensure Snowflake secrets are loaded
try:
    snowflake_config = st.secrets["snowflake"]
except KeyError:
    st.error("Snowflake configuration not found in secrets.toml. Ensure the secrets file is properly configured.")
    st.stop()

# Initialize Snowflake connection
if "CONN" not in st.session_state or st.session_state.CONN is None:
    try:
        st.session_state.CONN = snowflake.connector.connect(
            user=snowflake_config["user"],
            password=snowflake_config["password"],
            account=snowflake_config["account"],
            warehouse=snowflake_config["warehouse"],
            database=snowflake_config["database"],
            schema=snowflake_config["schema"],
            role=snowflake_config["role"],
        )
        st.success("Connected to Snowflake successfully!")
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        st.stop()


# Function to send a message to the REST API
def send_message(prompt: str) -> Dict[str, Any]:
    """Calls the REST API and returns the response."""
    try:
        request_body = {
            "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
            "semantic_model_file": f"@{snowflake_config['database']}.{snowflake_config['schema']}.{snowflake_config['stage']}/{snowflake_config['file']}"
        }
        resp = requests.post(
            url=f"https://{snowflake_config['host']}/api/v2/cortex/analyst/message",
            json=request_body,
            headers={
                "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
                "Content-Type": "application/json",
            },
        )
        request_id = resp.headers.get("X-Snowflake-Request-Id")
        if resp.status_code < 400:
            return {**resp.json(), "request_id": request_id}  # type: ignore[arg-type]
        else:
            st.error(f"Request failed with status {resp.status_code}: {resp.text}")
            return {}
    except Exception as e:
        st.error(f"Error sending message: {e}")
        return {}

# Function to process user input and generate responses
def process_message(prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(prompt=prompt)
            request_id = response.get("request_id")
            content = response.get("message", {}).get("content", [])
            display_content(content=content, request_id=request_id)  # type: ignore[arg-type]
    st.session_state.messages.append(
        {"role": "assistant", "content": content, "request_id": request_id}
    )

# Function to display content from the response
def display_content(
    content: List[Dict[str, str]],
    request_id: Optional[str] = None,
    message_index: Optional[int] = None,
) -> None:
    """Displays a content item for a message."""
    message_index = message_index or len(st.session_state.messages)
    if request_id:
        with st.expander("Request ID", expanded=False):
            st.markdown(request_id)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    df = pd.read_sql_query(item["statement"], st.session_state.CONN)
                    if len(df.index) > 1:
                        data_tab, line_tab, bar_tab = st.tabs(
                            ["Data", "Line Chart", "Bar Chart"]
                        )
                        data_tab.dataframe(df)
                        if len(df.columns) > 1:
                            df = df.set_index(df.columns[0])
                        with line_tab:
                            st.line_chart(df)
                        with bar_tab:
                            st.bar_chart(df)
                    else:
                        st.dataframe(df)

# Streamlit app title and UI
st.title("Cortex Analyst")
st.markdown(f"Semantic Model: `{snowflake_config['file']}`")

# Initialize session state for chat messages
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None

# Display existing messages
for message_index, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        display_content(
            content=message["content"],
            request_id=message.get("request_id"),
            message_index=message_index,
        )

# Process user input
if user_input := st.chat_input("What is your question?"):
    process_message(prompt=user_input)

# Handle active suggestions
if st.session_state.active_suggestion:
    process_message(prompt=st.session_state.active_suggestion)
    st.session_state.active_suggestion = None
