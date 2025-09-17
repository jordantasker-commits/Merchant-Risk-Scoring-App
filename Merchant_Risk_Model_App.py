import streamlit as st
import pandas as pd
# --- CHANGED: Import SQLAlchemy instead of Snowpark ---
from sqlalchemy import create_engine, text

# --- Page Configuration ---
st.set_page_config(
    page_title="Merchant Risk Review Workbench",
    page_icon="🛡️",
    layout="wide"
)

st.title("🛡️ Merchant Risk Review Workbench")
st.write("A tool to review, investigate, and update the status of high-risk merchants.")

try:
    # --- CHANGED: Establish connection using SQLAlchemy and st.secrets ---
    # This securely builds your connection string from your secrets.toml file.
    conn_info = st.secrets["connections"]["snowflake"]
    engine_url = (
        f"snowflake://{conn_info['user']}:{conn_info['password']}@{conn_info['account']}/"
        f"{conn_info['database']}/{conn_info['schema']}?warehouse={conn_info['warehouse']}&role={conn_info['role']}"
    )
    engine = create_engine(engine_url)

    # --- CHANGED: Get current user with a SQL query ---
    user_df = pd.read_sql("SELECT CURRENT_USER() as user", engine)
    # This version works regardless of the column name
    current_user = user_df.iloc[0, 0].replace("'", "''")

    # --- Monitoring Dashboard Section ---
    st.header("Weekly Review Analytics")


    @st.cache_data(ttl=600)
    def load_monitoring_data():
        query = """
            SELECT 
                DATE_TRUNC('week', WEEK_START_DATE)::DATE AS REVIEW_WEEK,
                STATUS,
                COUNT(*) AS MERCHANT_COUNT
            FROM ANALYTICS_TEAM.TEAM_ANALYTICS_BI.MERCHANT_REVIEW_STATUS
            GROUP BY 1, 2
            ORDER BY 1 DESC, 2;
        """
        # --- CHANGED: Use pandas to read SQL ---
        monitoring_df = pd.read_sql(query, engine)
        monitoring_df.columns = [col.upper() for col in monitoring_df.columns]
        return monitoring_df


    monitor_df = load_monitoring_data()
    if not monitor_df.empty:
        pivot_df = monitor_df.pivot(index='REVIEW_WEEK', columns='STATUS', values='MERCHANT_COUNT').fillna(0)
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Review Totals by Week")
            st.dataframe(pivot_df, use_container_width=True)
        with col2:
            st.subheader("Review Trend")
            st.bar_chart(pivot_df)
    else:
        st.info("No review data available yet.")

    st.divider()


    # --- Main App Logic (Review Queue) ---
    @st.cache_data
    def load_review_queue():
        query = """
            WITH latest_reviews AS (
                SELECT MERCHANT_DESCRIPTION, STATUS, REVIEW_DATE
                FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY MERCHANT_DESCRIPTION ORDER BY REVIEW_DATE DESC) as rn
                    FROM ANALYTICS_TEAM.TEAM_ANALYTICS_BI.MERCHANT_REVIEW_STATUS
                )
                WHERE rn = 1
            )
            SELECT s.MERCHANT_DESCRIPTION, s.RISK_SCORE, s.REASON_CODES, s.WEEK_START_DATE
            FROM ANALYTICS_TEAM.TEAM_ANALYTICS_BI.MERCHANT_RISK_SCORES s
            LEFT JOIN latest_reviews r ON s.MERCHANT_DESCRIPTION = r.MERCHANT_DESCRIPTION
            WHERE s.HIGH_RISK_MERCHANT::STRING ILIKE 'true' 
            AND (
                r.STATUS IS NULL OR 
                (r.STATUS = 'Reviewed - Benign' AND r.REVIEW_DATE <= DATEADD(day, -90, CURRENT_DATE())) OR
                r.STATUS = 'Pending Investigation'
            )
            ORDER BY s.RISK_SCORE DESC;
        """
        review_queue_df = pd.read_sql(query, engine)

        # --- NEW: Add this line to standardize column names to uppercase ---
        review_queue_df.columns = [col.upper() for col in review_queue_df.columns]

        return review_queue_df


    review_df = load_review_queue()
    if review_df.empty:
        st.success("🎉 Great job! The review queue is empty.")
        st.balloons()
    else:
        st.header("Merchant Review Queue")
        df_for_display = review_df[['WEEK_START_DATE', 'MERCHANT_DESCRIPTION', 'REASON_CODES']].rename(
            columns={'WEEK_START_DATE': 'Review Week'}
        )
        st.data_editor(df_for_display, use_container_width=True, key="review_queue_editor", disabled=True)
        st.subheader("Select a Merchant to Review")

        if not review_df['MERCHANT_DESCRIPTION'].empty:
            selected_merchant = st.selectbox(
                "Select a merchant from the queue:",
                options=review_df['MERCHANT_DESCRIPTION'].unique(),
                key="merchant_selector"
            )
            merchant_details = review_df[review_df['MERCHANT_DESCRIPTION'] == selected_merchant].iloc[0]
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Reason(s) for Flag:**")
                st.info(merchant_details['REASON_CODES'])
            with col2:
                with st.form("review_form"):
                    status = st.selectbox("Set Status:",
                                          ["Reviewed - Benign", "Reviewed - Blocked", "Pending Investigation"],
                                          index=None, placeholder="Choose an outcome...")
                    notes = st.text_area("Add Review Notes:")
                    submitted = st.form_submit_button("Submit Review")
                    if submitted:
                        if not status:
                            st.error("Please select a status.")
                        else:
                            # --- CHANGED: Use SQLAlchemy engine to execute the MERGE statement ---
                            with engine.connect() as con:
                                merge_sql = text(f"""
                                    MERGE INTO ANALYTICS_TEAM.TEAM_ANALYTICS_BI.MERCHANT_REVIEW_STATUS t
                                    USING (
                                        SELECT 
                                            '{merchant_details['MERCHANT_DESCRIPTION'].replace("'", "''")}' AS MERCHANT_DESCRIPTION,
                                            '{status}' AS STATUS,
                                            '{notes.replace("'", "''")}' AS NOTES,
                                            '{current_user}' AS REVIEWER,
                                            {merchant_details['RISK_SCORE']} AS RISK_SCORE,
                                            '{merchant_details['WEEK_START_DATE']}' AS WEEK_START_DATE
                                    ) s
                                    ON t.MERCHANT_DESCRIPTION = s.MERCHANT_DESCRIPTION AND t.WEEK_START_DATE = s.WEEK_START_DATE
                                    WHEN MATCHED THEN UPDATE SET t.STATUS = s.STATUS, t.NOTES = s.NOTES, t.REVIEWER = s.REVIEWER, t.REVIEW_DATE = CURRENT_TIMESTAMP()
                                    WHEN NOT MATCHED THEN INSERT (MERCHANT_DESCRIPTION, STATUS, NOTES, REVIEWER, RISK_SCORE, WEEK_START_DATE) VALUES (s.MERCHANT_DESCRIPTION, s.STATUS, s.NOTES, s.REVIEWER, s.RISK_SCORE, s.WEEK_START_DATE);
                                """)
                                con.execute(merge_sql)
                                con.commit()  # Make sure to commit the transaction

                            st.success(
                                f"Status for '{merchant_details['MERCHANT_DESCRIPTION']}' updated to '{status}'!")
                            st.cache_data.clear()
                            st.rerun()

except Exception as e:
    st.error("An error occurred during app startup:")
    st.exception(e)