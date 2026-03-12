import os
import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from pyathena import connect

load_dotenv()
st.set_page_config(layout="wide", page_title="Market Sentiment Lakehouse")

@st.cache_resource
def get_conn():
    return connect(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
        s3_staging_dir=f"s3://{os.getenv('AWS_S3_BUCKET_NAME')}/athena-results/",
        region_name='eu-south-1'
    )

@st.cache_data(ttl=600)
def load_all_data():
    conn = get_conn()
    query = "SELECT * FROM market_sentiment_db.sentiment_impact ORDER BY window_start ASC"
    df = pd.read_sql(query, conn)
    
    numeric_cols = [
        'avg_price', 'vwap', 'rvol', 'volatility', 
        'social_sentiment', 'news_sentiment', 'sentiment_gap', 'combined_sentiment'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['window_start'] = pd.to_datetime(df['window_start'])
    
    df_latest = df.sort_values('window_start').groupby('ticker').last().reset_index()
    return df, df_latest

try:
    df_hist, df_latest = load_all_data()
except Exception as e:
    st.error(f"Errore nel caricamento dati da Athena: {e}")
    st.stop()

st.sidebar.title("Navigation")
view = st.sidebar.radio("Select View", ["Market Overview", "Ticker Deep-Dive"])

if view == "Market Overview":
    st.title("Market Sentiment Overview")
    st.markdown("Analisi aggregata di tutti i 18 ticker nel Lakehouse. I dati volutamente simulano un mercato molto volatile. Ciò è utile a mostrare come il sistema intercetti correttamente il mercato e il suo contorno (social & news) e possa essere utilizato come base per future analisi complesse.")

    st.subheader("Top Performers (Sentiment)")
    top_cols = st.columns(5)
    top_sent = df_latest.nlargest(5, 'combined_sentiment')
    for i, row in enumerate(top_sent.itertuples()):
        top_cols[i].metric(row.ticker, f"${row.avg_price:.2f}", f"{row.combined_sentiment:.2f} Sent")

    st.markdown("---")
        
    st.subheader("Risk vs Sentiment Bubble Map")
    fig_risk = px.scatter(
        df_latest, x="combined_sentiment", y="volatility", size="rvol", 
        color="ticker", hover_name="ticker", template="plotly_dark",
        title="Size = Relative Volume (RVOL)"
    )
    st.plotly_chart(fig_risk, use_container_width=True)

    st.markdown("---")

    st.subheader("Sentiment Matrix")
    df_pivot = df_hist.pivot_table(index='window_start', columns='ticker', values='combined_sentiment', aggfunc='mean').fillna(0)
    fig_corr = px.imshow(df_pivot.corr(), text_auto=".1f", template="plotly_dark", color_continuous_scale="RdBu_r")
    st.plotly_chart(fig_corr, use_container_width=True)

    st.subheader("All Assets Deep-Dive")
    st.data_editor(df_latest[['ticker', 'avg_price', 'vwap', 'combined_sentiment', 'rvol', 'volatility']], use_container_width=True, hide_index=True)

else:
    st.sidebar.markdown("---")
    selected_asset = st.sidebar.selectbox("Choose Ticker", sorted(df_latest['ticker'].unique()))
    
    st.title(f"🔍 Deep-Dive: {selected_asset}")
    
    ticker_df = df_hist[df_hist['ticker'] == selected_asset]
    latest_val = df_latest[df_latest['ticker'] == selected_asset].iloc[0]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Last Price", f"${latest_val.avg_price:.2f}")
    m2.metric("VWAP", f"${latest_val.vwap:.2f}")
    m3.metric("RVOL", f"{latest_val.rvol:.2f}x")
    m4.metric("Sentiment", f"{latest_val.combined_sentiment:.2f}")

    st.subheader("Price & VWAP Evolution")
    fig_p = px.line(ticker_df, x="window_start", y=["avg_price", "vwap"], template="plotly_dark", color_discrete_map={"avg_price": "#00f2ff", "vwap": "#ff00ff"})
    st.plotly_chart(fig_p, use_container_width=True)

    st.subheader("Social vs News Sentiment Breakdown")
    fig_s = px.area(
        ticker_df, 
        x="window_start", 
        y=["social_sentiment", "news_sentiment"], 
        template="plotly_dark"
    )
    fig_s.update_traces(stackgroup=None) 
    st.plotly_chart(fig_s, use_container_width=True)

st.sidebar.info("Data loaded from AWS Athena (Gold Layer).")