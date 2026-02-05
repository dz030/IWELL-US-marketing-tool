import smtplib
import streamlit as st

# Pull from your actual secrets
email = st.secrets["smtp"]["SENDER_EMAIL"]
pwd = st.secrets["smtp"]["APP_PASSWORD"]

try:
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(email, pwd)
    print("🚀 THE GATE IS OPEN! Login successful.")
    server.quit()
except Exception as e:
    print(f"❌ Still blocked: {e}")