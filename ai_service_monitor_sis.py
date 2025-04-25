import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from datetime import datetime

# --- Page Configuration ---
st.set_page_config(
    page_title="Snowflake AI Services Usage",
    layout="wide"
)

# --- Streamlit App Layout ---

st.title("Snowflake AI Services Monitoring :brain:")

st.markdown("---")
st.markdown("""
*This data is exclusively AI Services spend and does not include credits consumed by warehouses running queries, storage for embeddings, app usage, or any other complimentary funcationality*

*Data sourced from `SNOWFLAKE.ACCOUNT_USAGE` views.*

*Note: `ACCOUNT_USAGE` data can have latency (up to a few hours).*

*Ensure the app's Snowflake Role has necessary SELECT privileges on relevant views.*
""")
current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')
st.caption(f"App data based on filter selection. App refreshed around: {current_time_str}")

# --- Get Snowflake Session ---
try:
    session = get_active_session()
    SESSION_AVAILABLE = True
except Exception as e:
    st.error(f"Could not get Snowflake session: {e}")
    st.warning("Ensure this app is running within Snowflake.")
    SESSION_AVAILABLE = False

# --- Function Definitions ---
@st.cache_data(ttl=600)
def fetch_ai_usage_data(days_limit):
    """(Chart 1) Fetches AI services usage data from Snowflake."""
    if not SESSION_AVAILABLE: return pd.DataFrame()
    query = f"""
    SELECT usage_date::date AS usage_date, credits_used
    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
    WHERE SERVICE_TYPE = 'AI_SERVICES'
      AND usage_date >= DATEADD(day, -{int(days_limit)}, CURRENT_DATE())
    ORDER BY usage_date DESC
    LIMIT {int(days_limit)};
    """
    try:
        snowpark_df = session.sql(query)
        pd_df = snowpark_df.to_pandas()
        if pd_df.empty: return pd.DataFrame() # Return empty if no data
        pd_df.columns = [col.lower() for col in pd_df.columns]
        pd_df['usage_date'] = pd.to_datetime(pd_df['usage_date'])
        pd_df = pd_df.sort_values(by='usage_date', ascending=True)
        return pd_df
    except Exception as e:
        st.error(f"(Chart 1) Error fetching AI Services data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def fetch_llm_function_usage(days_limit):
    """(Tab 2.1) Fetches LLM function usage (token credits) from Snowflake."""
    if not SESSION_AVAILABLE: return pd.DataFrame()
    query = f"""
    SELECT date(qh.start_time) as usage_date, cf.function_name, sum(cf.token_credits) as total_token_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY as cf
    JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY as qh ON qh.query_id = cf.query_id
    WHERE date(qh.start_time) >= DATEADD(day, -{int(days_limit)}, CURRENT_DATE())
      AND cf.function_name IS NOT NULL AND cf.token_credits > 0
    GROUP BY 1, 2 ORDER BY 1, 2;
    """
    try:
        snowpark_df = session.sql(query)
        pd_df = snowpark_df.to_pandas()
        if pd_df.empty: return pd.DataFrame()
        pd_df.columns = [col.lower() for col in pd_df.columns]
        pd_df['usage_date'] = pd.to_datetime(pd_df['usage_date'])
        return pd_df
    except Exception as e:
        st.error(f"(Tab 2.1) Error fetching LLM Function Usage data: {e}")
        st.warning("Ensure role has SELECT on CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY and QUERY_HISTORY.")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def fetch_complete_model_usage(days_limit):
    """(Tab 2.2) Fetches 'COMPLETE' function usage by model from Snowflake."""
    if not SESSION_AVAILABLE: return pd.DataFrame()
    query = f"""
    SELECT date(qh.start_time) as usage_date, cf.model_name, sum(cf.token_credits) as total_token_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY as cf
    JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY as qh ON qh.query_id = cf.query_id
    WHERE cf.function_name = 'COMPLETE'
      AND date(qh.start_time) >= DATEADD(day, -{int(days_limit)}, CURRENT_DATE())
      AND cf.token_credits > 0
    GROUP BY 1, 2 ORDER BY 1, 2;
    """
    try:
        snowpark_df = session.sql(query)
        pd_df = snowpark_df.to_pandas()
        if pd_df.empty: return pd.DataFrame()
        pd_df.columns = [col.lower() for col in pd_df.columns]
        pd_df['usage_date'] = pd.to_datetime(pd_df['usage_date'])
        return pd_df
    except Exception as e:
        st.error(f"(Tab 2.2) Error fetching 'COMPLETE' usage data: {e}")
        st.warning("Ensure role has SELECT on CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY and QUERY_HISTORY.")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def fetch_cortex_search_by_service(days_limit):
    """(Tab 3.1) Fetches Cortex Search usage by service name."""
    if not SESSION_AVAILABLE: return pd.DataFrame()
    # st.info(f"(Tab 3.1) Fetching Cortex Search Usage by Service for the last {days_limit} days...")
    query = f"""
    SELECT
        usage_date::date as usage_date,
        service_name,
        sum(credits) as total_credits -- Use credits_used if that's the column name, else credits
    FROM
        SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY -- Adjusted table name based on likely pattern
    WHERE
        usage_date >= DATEADD(day, -{int(days_limit)}, CURRENT_DATE())
    GROUP BY
        1, 2
    ORDER BY
        1, 2;
    """
    try:
        snowpark_df = session.sql(query)
        pd_df = snowpark_df.to_pandas()
        if pd_df.empty: return pd.DataFrame()
        pd_df.columns = [col.lower() for col in pd_df.columns]
        pd_df['usage_date'] = pd.to_datetime(pd_df['usage_date'])
        # st.success("(Tab 3.1) Cortex Search by Service data fetched.")
        return pd_df
    except Exception as e:
        st.error(f"(Tab 3.1) Error fetching Cortex Search by Service data: {e}")
        st.warning("Ensure role has SELECT on CORTEX_SEARCH_DAILY_USAGE_HISTORY.")
        st.error(f"Query attempted:\n```sql\n{query}\n```") # Show query on error
        return pd.DataFrame()

@st.cache_data(ttl=600)
def fetch_cortex_search_by_consumption(days_limit):
    """(Tab 3.2) Fetches Cortex Search usage by consumption type."""
    if not SESSION_AVAILABLE: return pd.DataFrame()
    # st.info(f"(Tab 3.2) Fetching Cortex Search Usage by Consumption Type for the last {days_limit} days...")
    # Corrected query based on user input and likely schema - assuming table name is CORTEX_SEARCH_SERVICE_USAGE_HISTORY
    query = f"""
    SELECT
        usage_date::date as usage_date,
        service_name,
        'QUERY' as consumption_type, -- Example: Assuming consumption type needs mapping or isn't direct
        sum(credits) as total_credits -- Adjust column names as needed
    FROM
        SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY -- Adjusted table name
    WHERE
        usage_date >= DATEADD(day, -{int(days_limit)}, CURRENT_DATE())
    GROUP BY
        1, 2, 3 -- Group by date, service, consumption type
    ORDER BY
        1, 2, 3; -- Order by date, service, consumption type
    """
    try:
        snowpark_df = session.sql(query)
        pd_df = snowpark_df.to_pandas()
        if pd_df.empty: return pd.DataFrame()
        pd_df.columns = [col.lower() for col in pd_df.columns]
        pd_df['usage_date'] = pd.to_datetime(pd_df['usage_date'])
        # st.success("(Tab 3.2) Cortex Search by Consumption data fetched.")
        return pd_df
    except Exception as e:
        st.error(f"(Tab 3.2) Error fetching Cortex Search by Consumption data: {e}")
        st.warning("Ensure role has SELECT on the correct Cortex Search usage history view.")
        st.error(f"Query attempted:\n```sql\n{query}\n```") # Show query on error
        return pd.DataFrame()


@st.cache_data(ttl=600)
def fetch_cortex_analyst_usage(days_limit):
    """(Section 4) Fetches aggregated Cortex Analyst usage."""
    if not SESSION_AVAILABLE: return pd.DataFrame()
    # st.info(f"(Section 4) Fetching Cortex Analyst usage for the last {days_limit} days...")

    # ** IMPORTANT: Verify column names 'USAGE_DATE' and 'CREDITS_USED' below **
    # This query aggregates credits by day. Original query was SELECT *.
    query = f"""
    SELECT
       date(start_time) as usage_date, -- Assuming USAGE_DATE exists, adjust if needed (e.g., DATE(START_TIME))
        sum(credits) as total_credits -- Assuming CREDITS_USED exists, adjust if needed
    FROM
        SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
    WHERE
        USAGE_DATE >= DATEADD(day, -{int(days_limit)}, CURRENT_DATE()) -- Adjust date column if needed
    GROUP BY
        1
    ORDER BY
        1;
    """
    try:
        snowpark_df = session.sql(query)
        pd_df = snowpark_df.to_pandas()
        if pd_df.empty:
            # st.success("(Section 4) Cortex Analyst usage data fetched (No usage found).")
            return pd.DataFrame()
        pd_df.columns = [col.lower() for col in pd_df.columns] # Ensure lowercase columns
        pd_df['usage_date'] = pd.to_datetime(pd_df['usage_date']) # Ensure date type
        # st.success("(Section 4) Cortex Analyst usage data fetched successfully!")
        return pd_df
    except Exception as e:
        st.error(f"(Section 4) Error fetching Cortex Analyst usage data: {e}")
        st.warning("Ensure role has SELECT on CORTEX_ANALYST_USAGE_HISTORY and verify column names (e.g., USAGE_DATE, CREDITS_USED).")
        st.error(f"Query attempted:\n```sql\n{query}\n```") # Show query on error
        return pd.DataFrame()

@st.cache_data(ttl=600)
def fetch_document_ai_usage(days_limit):
    """(Section 5) Fetches aggregated Document AI usage."""
    if not SESSION_AVAILABLE: return pd.DataFrame()
    # st.info(f"(Section 5) Fetching Document AI usage for the last {days_limit} days...")

    # Adding timeframe filter and alias to the provided query
    # ** IMPORTANT: Verify column names 'start_time' and 'credits_used' below **
    query = f"""
    SELECT
        date(start_time) as usage_date,
        sum(credits_used) as total_credits
    FROM
        snowflake.account_usage.DOCUMENT_AI_USAGE_HISTORY
    WHERE
        date(start_time) >= DATEADD(day, -{int(days_limit)}, CURRENT_DATE()) -- Added timeframe filter
    GROUP BY
        1
    ORDER BY
        1;
    """
    try:
        snowpark_df = session.sql(query)
        pd_df = snowpark_df.to_pandas()
        if pd_df.empty:
            # st.success("(Section 5) Document AI usage data fetched (No usage found).")
            return pd.DataFrame()
        # Ensure columns are lowercase, matching the explicit aliases in the query
        pd_df.columns = ['usage_date', 'total_credits']
        pd_df['usage_date'] = pd.to_datetime(pd_df['usage_date']) # Ensure date type
        # st.success("(Section 5) Document AI usage data fetched successfully!")
        return pd_df
    except Exception as e:
        st.error(f"(Section 5) Error fetching Document AI usage data: {e}")
        st.warning("Ensure role has SELECT on DOCUMENT_AI_USAGE_HISTORY and verify column names (e.g., start_time, credits_used).")
        st.error(f"Query attempted:\n```sql\n{query}\n```") # Show query on error
        return pd.DataFrame()



if SESSION_AVAILABLE:
     st.caption(f"Running as role: `{session.get_current_role()}` | Using warehouse: `{session.get_current_warehouse()}`")
else:
     st.stop() # Stop if no session

# --- User Input: Dropdown for days limit ---
days_options = [7, 14, 30, 60, 90]
selected_days = st.selectbox(
    "Select the number of recent days to display:",
    options=days_options,
    index=2 # Default to 30 days
)

# --- Fetch all data upfront based on selection ---
df_ai_usage = fetch_ai_usage_data(selected_days)
df_llm_usage = fetch_llm_function_usage(selected_days)
df_complete_usage = fetch_complete_model_usage(selected_days)
df_search_service = fetch_cortex_search_by_service(selected_days)
df_search_consumption = fetch_cortex_search_by_consumption(selected_days)
df_analyst_usage = fetch_cortex_analyst_usage(selected_days)
df_docai_usage = fetch_document_ai_usage(selected_days)




# --- Display Charts ---

# --- Section 1: AI Services Usage ---
st.markdown("---")
st.header("All AI Services Usage")
if not df_ai_usage.empty:
    chart1 = alt.Chart(df_ai_usage).mark_bar().encode(
        x=alt.X('usage_date', title='Date', axis=alt.Axis(format="%Y-%m-%d")),
        y=alt.Y('credits_used', title='Credits Used'),
        tooltip=[ alt.Tooltip('usage_date', title='Date', format="%Y-%m-%d"), alt.Tooltip('credits_used', title='Credits Used', format=".2f") ]
    ).properties(width=alt.Step(40)).interactive()
    st.altair_chart(chart1, use_container_width=True)
    with st.expander("View AI Services Raw Data (Chart 1)"):
        st.dataframe(df_ai_usage.style.format({"usage_date": "{:%Y-%m-%d}", "credits_used": "{:.2f}"}), use_container_width=True)
else:
    if 'fetch_ai_usage_data' not in st.session_state or not hasattr(st.session_state.fetch_ai_usage_data, 'exception'):
         st.write("No AI Services usage data found for the selected period.")

# --- Section 2: LLM Usage Details (in Tabs) ---
st.markdown("---")
st.header("Detailed LLM Function Usage")
llm_tab1, llm_tab2 = st.tabs(["All LLM Functions", "'COMPLETE' by Model"])

with llm_tab1:
    st.subheader("Usage by Function Name")
    if not df_llm_usage.empty:
        chart2 = alt.Chart(df_llm_usage).mark_bar().encode(
            x=alt.X('usage_date', title='Date', axis=alt.Axis(format="%Y-%m-%d")),
            y=alt.Y('total_token_credits', title='Total Token Credits'),
            color=alt.Color('function_name', title='Function Name'),
            order=alt.Order('total_token_credits', sort='descending'),
            tooltip=[ alt.Tooltip('usage_date', title='Date', format="%Y-%m-%d"), alt.Tooltip('function_name', title='Function Name'), alt.Tooltip('total_token_credits', title='Token Credits', format=",.0f") ]
        ).interactive()
        st.altair_chart(chart2, use_container_width=True)
        with st.expander("View All LLM Function Usage Raw Data"):
            st.dataframe(df_llm_usage.style.format({"usage_date": "{:%Y-%m-%d}", "total_token_credits": "{:,.0f}"}), use_container_width=True)
    else:
        if 'fetch_llm_function_usage' not in st.session_state or not hasattr(st.session_state.fetch_llm_function_usage, 'exception'):
             st.write("No LLM Function usage data found for the selected period.")

with llm_tab2:
    st.subheader("Usage for 'COMPLETE' Function by Model")
    if not df_complete_usage.empty:
        chart3 = alt.Chart(df_complete_usage).mark_bar().encode(
            x=alt.X('usage_date', title='Date', axis=alt.Axis(format="%Y-%m-%d")),
            y=alt.Y('total_token_credits', title='Total Token Credits'),
            color=alt.Color('model_name', title='Model Name'),
            order=alt.Order('total_token_credits', sort='descending'),
            tooltip=[ alt.Tooltip('usage_date', title='Date', format="%Y-%m-%d"), alt.Tooltip('model_name', title='Model Name'), alt.Tooltip('total_token_credits', title='Token Credits (COMPLETE)', format=",.0f") ]
        ).interactive()
        st.altair_chart(chart3, use_container_width=True)
        with st.expander("View 'COMPLETE' Usage Raw Data"):
             st.dataframe(df_complete_usage.style.format({"usage_date": "{:%Y-%m-%d}", "total_token_credits": "{:,.0f}"}), use_container_width=True)
    else:
        if 'fetch_complete_model_usage' not in st.session_state or not hasattr(st.session_state.fetch_complete_model_usage, 'exception'):
             st.write("No 'COMPLETE' function usage data found for the selected period.")


# --- Section 3: Cortex Search Usage (in Tabs) ---
st.markdown("---")
st.header("Cortex Search Service Usage")

search_tab1, search_tab2 = st.tabs(["Usage by Service", "Usage by Consumption Type"])

# --- Tab 3.1: Cortex Search by Service ---
with search_tab1:
    st.subheader("Daily Credits by Service Name")
    if not df_search_service.empty:
        chart4 = alt.Chart(df_search_service).mark_bar().encode(
            x=alt.X('usage_date', title='Date', axis=alt.Axis(format="%Y-%m-%d")),
            y=alt.Y('total_credits', title='Total Credits Used'),
            color=alt.Color('service_name', title='Service Name'),
            order=alt.Order('total_credits', sort='descending'),
            tooltip=[
                alt.Tooltip('usage_date', title='Date', format="%Y-%m-%d"),
                alt.Tooltip('service_name', title='Service Name'),
                alt.Tooltip('total_credits', title='Credits Used', format=".2f")
            ]
        ).interactive()
        st.altair_chart(chart4, use_container_width=True)
        with st.expander("View Cortex Search by Service Raw Data"):
            st.dataframe(df_search_service.style.format({"usage_date": "{:%Y-%m-%d}", "total_credits": "{:.2f}"}), use_container_width=True)
    else:
        # Display message only if data fetching didn't raise an error but returned empty
        if 'fetch_cortex_search_by_service' not in st.session_state or not hasattr(st.session_state.fetch_cortex_search_by_service, 'exception'):
            st.write("No Cortex Search usage data found for the selected period.")


# --- Tab 3.2: Cortex Search by Consumption Type (with Filter) ---
with search_tab2:
    st.subheader("Consumption Type for each Service")
    if not df_search_consumption.empty:
        # --- Service Name Filter Dropdown ---
        # Get unique service names from the dataframe for the filter
        service_names = ['All Services'] + sorted(df_search_consumption['service_name'].unique())
        selected_service = st.selectbox(
            "Filter by Service Name:",
            options=service_names,
            key='cortex_service_filter' # Unique key for this selectbox
        )

        # --- Filter Data ---
        if selected_service == 'All Services':
            filtered_df_consumption = df_search_consumption
        else:
            filtered_df_consumption = df_search_consumption[df_search_consumption['service_name'] == selected_service]

        # --- Display Chart for Filtered Data ---
        if not filtered_df_consumption.empty:
            chart5 = alt.Chart(filtered_df_consumption).mark_bar().encode(
                x=alt.X('usage_date', title='Date', axis=alt.Axis(format="%Y-%m-%d")),
                y=alt.Y('total_credits', title='Total Credits Used'),
                # Stack by consumption_type
                color=alt.Color('consumption_type', title='Consumption Type'),
                order=alt.Order('total_credits', sort='descending'),
                tooltip=[
                    alt.Tooltip('usage_date', title='Date', format="%Y-%m-%d"),
                    alt.Tooltip('service_name', title='Service Name'),
                    alt.Tooltip('consumption_type', title='Consumption Type'),
                    alt.Tooltip('total_credits', title='Credits Used', format=".2f")
                ]
            ).interactive()
            st.altair_chart(chart5, use_container_width=True)
            with st.expander(f"View Filtered Cortex Search by Consumption Raw Data (Service: {selected_service})"):
                st.dataframe(filtered_df_consumption.style.format({"usage_date": "{:%Y-%m-%d}", "total_credits": "{:.2f}"}), use_container_width=True)
        else:
            st.write(f"No Cortex Search consumption data found for Service Name '{selected_service}' in the selected period.")

    else:
        # Display message only if data fetching didn't raise an error but returned empty
        if 'fetch_cortex_search_by_consumption' not in st.session_state or not hasattr(st.session_state.fetch_cortex_search_by_consumption, 'exception'):
            st.write("No Cortex Search consumption data found for the selected period.")

# --- Section 4: Cortex Analyst Usage ---
st.markdown("---")
st.header("Cortex Analyst Usage")
if not df_analyst_usage.empty:
    st.subheader("Daily Credits Used by Cortex Analyst")
    chart6 = alt.Chart(df_analyst_usage).mark_bar().encode(
        x=alt.X('usage_date', title='Date', axis=alt.Axis(format="%Y-%m-%d")),
        y=alt.Y('total_credits', title='Total Credits Used'),
        tooltip=[
            alt.Tooltip('usage_date', title='Date', format="%Y-%m-%d"),
            alt.Tooltip('total_credits', title='Credits Used', format=".2f")
        ]
    ).properties(
        width=alt.Step(40) # Consistent bar width with chart 1
    ).interactive()
    st.altair_chart(chart6, use_container_width=True)

    # Expander shows the aggregated data used for the chart
    with st.expander("View Aggregated Cortex Analyst Raw Data"):
        st.dataframe(
            df_analyst_usage.style.format({"usage_date": "{:%Y-%m-%d}", "total_credits": "{:.2f}"}),
            use_container_width=True
        )
    # Optional: Add a way to view non-aggregated data if needed, but be mindful of size.
    # with st.expander("View Detailed Cortex Analyst History (Potentially Large)"):
    #     # You might add another fetch function here that does SELECT * with a date filter
    #     st.write("Detailed view functionality not implemented yet.")

else:
    # Display message only if data fetching didn't raise an error but returned empty
    if 'fetch_cortex_analyst_usage' not in st.session_state or not hasattr(st.session_state.fetch_cortex_analyst_usage, 'exception'):
         st.write("No Cortex Analyst usage data found for the selected period.")

# --- Section 5: Document AI Usage ---
st.markdown("---")
st.header("Document AI Usage")
if not df_docai_usage.empty:
    st.subheader("Daily Credits Used by Document AI")
    chart7 = alt.Chart(df_docai_usage).mark_bar().encode(
        x=alt.X('usage_date', title='Date', axis=alt.Axis(format="%Y-%m-%d")),
        y=alt.Y('total_credits', title='Total Credits Used'),
        tooltip=[
            alt.Tooltip('usage_date', title='Date', format="%Y-%m-%d"),
            alt.Tooltip('total_credits', title='Credits Used', format=".2f")
        ]
    ).properties(
        width=alt.Step(40) # Consistent bar width
    ).interactive()
    st.altair_chart(chart7, use_container_width=True)

    # Expander shows the aggregated data used for the chart
    with st.expander("View Aggregated Document AI Raw Data"):
        st.dataframe(
            df_docai_usage.style.format({"usage_date": "{:%Y-%m-%d}", "total_credits": "{:.2f}"}),
            use_container_width=True
        )
else:
    if 'fetch_document_ai_usage' not in st.session_state or not hasattr(st.session_state.fetch_document_ai_usage, 'exception'):
         st.write("No Document AI usage data found for the selected period.")
