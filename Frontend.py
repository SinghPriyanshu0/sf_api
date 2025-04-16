import streamlit as st
import pandas as pd
from Backend import get_connection
from snowflake.connector.errors import ProgrammingError

st.set_page_config(page_title="Search in Snowflake Tables", layout="centered")
st.title("🔍 Search Records in Snowflake")

# Input fields
email_input = st.text_input("Enter Email")
phone_input = st.text_input("Enter Phone Number")

def search_records(email, phone):
    results = {}
    try:
        conn = get_connection()
        cur = conn.cursor()
        tables = ['Table1', 'Table2', 'Table3']

        for table in tables:
            query = f"""
                SELECT * FROM {table}
                WHERE Email = %s AND MobilePhone = %s
            """
            cur.execute(query, (email, phone))
            rows = cur.fetchall()
            if rows:
                colnames = [desc[0] for desc in cur.description]
                results[table] = pd.DataFrame(rows, columns=colnames)
        cur.close()
        conn.close()
    except ProgrammingError as e:
        st.error(f"❌ Programming error: {e}")
    except Exception as e:
        st.error(f"❌ Failed to search: {e}")
    return results

if st.button("Search"):
    if not email_input or not phone_input:
        st.warning("Please enter both email and phone number.")
    else:
        with st.spinner("Searching..."):
            result_tables = search_records(email_input, phone_input)
            if not result_tables:
                st.info("No matching records found in any table.")
            else:
                for table, df in result_tables.items():
                    st.subheader(f"✅ Match found in {table}")
                    st.dataframe(df)


