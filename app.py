import streamlit as st
from apify_client import ApifyClient
from dotenv import load_dotenv
import os
from typing import List, Dict, Any
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import ssl
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import time
import random
from streamlit_option_menu import option_menu
import pandas as pd

# SSL Bypass for Mac (Keep this)
ssl._create_default_https_context = ssl._create_unverified_context

# Google Sheets Setup constants
SHEET_ID = "1yCmsXkTgNVvxAXFa_ZpLLXWNOZgDipVEoT4rXOD4HmY"
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# --- NEW CLOUD-READY AUTH LOGIC ---

# 1. Handle APIFY Token
# It looks in st.secrets first (Cloud), then falls back to .env (Local)
apify_token = st.secrets.get("APIFY_API_TOKEN") or os.getenv("APIFY_API_TOKEN")
client = ApifyClient(apify_token)

# 2. Handle Google Credentials
def get_google_creds():
    # 1. Try to get credentials from Streamlit Cloud Secrets
    if "gcp_service_account" in st.secrets:
        return Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]), 
            scopes=SCOPE
        )
    # 2. Fallback to local file for Cursor testing
    elif os.path.exists("service_account.json"):
        return Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
    else:
        st.error("No Google credentials found in Secrets or local file!")
        return None


def get_gspread_client():
    """Get authorized gspread client using the correct secret key."""
    try:
        # 1. Try Cloud Secrets (Standard for Streamlit Cloud)
        if "gcp_service_account" in st.secrets:
            # We must cast to dict to ensure gspread can read it
            creds_info = dict(st.secrets["gcp_service_account"])
            creds = Credentials.from_service_account_info(creds_info, scopes=SCOPE)
            return gspread.authorize(creds)
        
        # 2. Fallback to local JSON file for Cursor testing
        elif os.path.exists("service_account.json"):
            creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
            return gspread.authorize(creds)
            
        else:
            st.error("No Google credentials found in st.secrets or local file.")
            return None
    except Exception as e:
        st.error(f"Critical Connection Error: {e}")
        return None

@st.cache_data
def fetch_templates():
    """
    Fetch email and DM templates from the '(DO NOT USE)Email Template' tab.
    Returns a list of template dictionaries, each with 'campaign_name', 'subject', 'email', and 'dm' keys.
    """
    try:
        gc = get_gspread_client()
        if not gc:
            return []
        
        sh = gc.open_by_key(SHEET_ID)
        
        # Try to access the template tab
        try:
            worksheet = sh.worksheet("(DO NOT USE)Email Template")
        except Exception:
            # Tab doesn't exist, return empty list
            return []
        
        # Get all rows
        all_values = worksheet.get_all_values()
        
        if not all_values or len(all_values) < 2:
            return []
        
        # Get headers
        headers = all_values[0]
        
        # Find column indices - order: Campaign Name, Email Subject, HTML Template, DM Template
        campaign_col_idx = None
        subject_col_idx = None
        html_col_idx = None
        dm_col_idx = None
        
        for i, header in enumerate(headers):
            header_lower = header.lower()
            if 'campaign' in header_lower and 'name' in header_lower:
                campaign_col_idx = i
            elif 'subject' in header_lower and 'email' in header_lower:
                subject_col_idx = i
            elif 'html' in header_lower and 'dm' not in header_lower:
                html_col_idx = i
            elif 'dm' in header_lower and 'template' in header_lower:
                dm_col_idx = i
        
        if campaign_col_idx is None:
            return []
        
        # Build list of templates
        templates = []
        for row in all_values[1:]:
            if len(row) > campaign_col_idx:
                campaign_name = row[campaign_col_idx].strip() if campaign_col_idx < len(row) else ''
                if campaign_name:
                    subject_content = row[subject_col_idx].strip() if subject_col_idx is not None and subject_col_idx < len(row) else ''
                    html_content = row[html_col_idx].strip() if html_col_idx is not None and html_col_idx < len(row) else ''
                    dm_content = row[dm_col_idx].strip() if dm_col_idx is not None and dm_col_idx < len(row) else ''
                    templates.append({
                        'campaign_name': campaign_name,
                        'subject': subject_content,
                        'email': html_content,
                        'dm': dm_content
                    })
        
        return templates
    except Exception as e:
        st.error(f"Error fetching templates: {e}")
        return []

def get_templates_by_campaign(campaign_name: str):
    """
    Get all templates for a specific campaign name.
    Returns a list of template dictionaries.
    """
    all_templates = fetch_templates()
    return [t for t in all_templates if t.get('campaign_name', '').strip() == campaign_name.strip()]

def get_global_templates():
    """Get all templates where Campaign Name is 'Global'."""
    return get_templates_by_campaign('Global')

def save_template_to_sheet(campaign_name: str, email_subject: str, html_content: str, dm_content: str, old_subject: str = None):
    """
    Save or update email subject, HTML template, and DM template in the '(DO NOT USE)Email Template' tab.
    Column order: Campaign Name, Email Subject, HTML Template, DM Template
    
    If old_subject is provided, updates the row matching campaign_name + old_subject.
    Otherwise, if a row with campaign_name + email_subject exists, updates it; otherwise appends a new row.
    
    Args:
        campaign_name: Name of the campaign
        email_subject: Email subject line
        html_content: HTML template content
        dm_content: DM template content
        old_subject: (Optional) Previous subject line when updating an existing template
        
    Returns:
        True if successful, False otherwise
    """
    try:
        gc = get_gspread_client()
        if not gc:
            return False
        
        sh = gc.open_by_key(SHEET_ID)
        
        # Try to access the template tab, create if it doesn't exist
        try:
            worksheet = sh.worksheet("(DO NOT USE)Email Template")
        except Exception:
            # Create the tab if it doesn't exist
            worksheet = sh.add_worksheet(title="(DO NOT USE)Email Template", rows=100, cols=20)
            # Add headers in the correct order
            headers = ['Campaign Name', 'Email Subject', 'HTML Template', 'DM Template']
            worksheet.append_row(headers)
        
        # Get all rows
        all_values = worksheet.get_all_values()
        
        # Get headers
        if not all_values or len(all_values) == 0:
            headers = ['Campaign Name', 'Email Subject', 'HTML Template', 'DM Template']
            worksheet.append_row(headers)
            all_values = [headers]
        
        headers = all_values[0]
        
        # Find column indices - order: Campaign Name, Email Subject, HTML Template, DM Template
        campaign_col_idx = None
        subject_col_idx = None
        html_col_idx = None
        dm_col_idx = None
        
        for i, header in enumerate(headers):
            header_lower = header.lower()
            if 'campaign' in header_lower and 'name' in header_lower:
                campaign_col_idx = i
            elif 'subject' in header_lower and 'email' in header_lower:
                subject_col_idx = i
            elif 'html' in header_lower and 'dm' not in header_lower:
                html_col_idx = i
            elif 'dm' in header_lower and 'template' in header_lower:
                dm_col_idx = i
        
        # If columns not found, use defaults (order: Campaign Name, Email Subject, HTML Template, DM Template)
        if campaign_col_idx is None:
            campaign_col_idx = 0
        if subject_col_idx is None:
            subject_col_idx = 1
        if html_col_idx is None:
            html_col_idx = 2
        if dm_col_idx is None:
            dm_col_idx = 3
        
        # Search for existing row - match by campaign_name AND subject
        # If old_subject is provided, use it for matching; otherwise use email_subject
        search_subject = old_subject if old_subject else email_subject
        found_row = None
        for idx, row in enumerate(all_values[1:], start=2):  # Start at row 2 (1-indexed)
            if len(row) > max(campaign_col_idx, subject_col_idx):
                row_campaign = row[campaign_col_idx].strip() if campaign_col_idx < len(row) else ''
                row_subject = row[subject_col_idx].strip() if subject_col_idx < len(row) else ''
                if row_campaign == campaign_name and row_subject == search_subject:
                    found_row = idx
                    break
        
        # Prepare row data - ensure it has enough columns
        max_cols = max(len(headers), campaign_col_idx + 1, subject_col_idx + 1, html_col_idx + 1, dm_col_idx + 1)
        row_data = [''] * max_cols
        row_data[campaign_col_idx] = campaign_name
        row_data[subject_col_idx] = email_subject
        row_data[html_col_idx] = html_content
        row_data[dm_col_idx] = dm_content
        
        if found_row:
            # Update existing row - use range notation
            end_col = chr(65 + max_cols - 1) if max_cols <= 26 else 'Z'
            range_name = f'A{found_row}:{end_col}{found_row}'
            worksheet.update(range_name, [row_data])
        else:
            # Append new row
            worksheet.append_row(row_data)
        
        # Clear the cache
        fetch_templates.clear()
        
        return True
    except Exception as e:
        st.error(f"Error saving template: {e}")
        return False

def save_dm_template_to_sheet(campaign_name: str, dm_content: str):
    """
    Save or update a DM template in the '(DO NOT USE)Email Template' tab.
    
    Args:
        campaign_name: Name of the campaign
        dm_content: DM template content (plain text, preserves line breaks)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        gc = get_gspread_client()
        if not gc:
            return False
        
        sh = gc.open_by_key(SHEET_ID)
        
        # Try to access the template tab, create if it doesn't exist
        try:
            worksheet = sh.worksheet("(DO NOT USE)Email Template")
        except Exception:
            # Create the tab if it doesn't exist
            worksheet = sh.add_worksheet(title="(DO NOT USE)Email Template", rows=100, cols=20)
            # Add headers
            headers = ['Campaign Name', 'HTML Template', 'DM Template']
            worksheet.append_row(headers)
        
        # Get all rows
        all_values = worksheet.get_all_values()
        
        # Get headers
        if not all_values or len(all_values) == 0:
            headers = ['Campaign Name', 'HTML Template', 'DM Template']
            worksheet.append_row(headers)
            all_values = [headers]
        
        headers = all_values[0]
        
        # Find column indices
        campaign_col_idx = None
        html_col_idx = None
        dm_col_idx = None
        
        for i, header in enumerate(headers):
            header_lower = header.lower()
            if 'campaign' in header_lower and 'name' in header_lower:
                campaign_col_idx = i
            elif 'html' in header_lower and 'dm' not in header_lower:
                html_col_idx = i
            elif 'dm' in header_lower and 'template' in header_lower:
                dm_col_idx = i
        
        # If columns not found, use defaults
        if campaign_col_idx is None:
            campaign_col_idx = 0
        if html_col_idx is None:
            html_col_idx = 1
        if dm_col_idx is None:
            dm_col_idx = 2
        
        # Search for existing row with this campaign name
        found_row = None
        for idx, row in enumerate(all_values[1:], start=2):  # Start at row 2 (1-indexed)
            if len(row) > campaign_col_idx and row[campaign_col_idx].strip() == campaign_name:
                found_row = idx
                break
        
        # Prepare row data - ensure it has enough columns
        max_cols = max(len(headers), campaign_col_idx + 1, html_col_idx + 1, dm_col_idx + 1)
        row_data = [''] * max_cols
        row_data[campaign_col_idx] = campaign_name
        row_data[dm_col_idx] = dm_content  # DM content preserves line breaks as plain text
        # Preserve existing HTML template if updating
        if found_row:
            existing_row = all_values[found_row - 1]  # found_row is 1-indexed, all_values is 0-indexed
            if len(existing_row) > html_col_idx:
                row_data[html_col_idx] = existing_row[html_col_idx]
        
        if found_row:
            # Update existing row - use range notation
            end_col = chr(65 + max_cols - 1) if max_cols <= 26 else 'Z'
            range_name = f'A{found_row}:{end_col}{found_row}'
            worksheet.update(range_name, [row_data])
        else:
            # Append new row
            worksheet.append_row(row_data)
        
        # Clear the cache
        fetch_templates.clear()
        
        return True
    except Exception as e:
        st.error(f"Error saving DM template: {e}")
        return False

def delete_template_from_sheet(campaign_name: str, email_subject: str):
    """
    Delete a template from the '(DO NOT USE)Email Template' tab.
    Matches by campaign_name AND email_subject.
    
    Args:
        campaign_name: Name of the campaign
        email_subject: Email subject line of the template to delete
        
    Returns:
        True if successful, False otherwise
    """
    try:
        gc = get_gspread_client()
        if not gc:
            return False
        
        sh = gc.open_by_key(SHEET_ID)
        
        try:
            worksheet = sh.worksheet("(DO NOT USE)Email Template")
        except Exception:
            return False
        
        # Get all rows
        all_values = worksheet.get_all_values()
        
        if not all_values or len(all_values) < 2:
            return False
        
        headers = all_values[0]
        
        # Find column indices
        campaign_col_idx = None
        subject_col_idx = None
        
        for i, header in enumerate(headers):
            header_lower = header.lower()
            if 'campaign' in header_lower and 'name' in header_lower:
                campaign_col_idx = i
            elif 'subject' in header_lower and 'email' in header_lower:
                subject_col_idx = i
        
        if campaign_col_idx is None or subject_col_idx is None:
            return False
        
        # Find the row to delete (matching both campaign_name and email_subject)
        row_to_delete = None
        for idx, row in enumerate(all_values[1:], start=2):  # Start at row 2 (1-indexed)
            if len(row) > max(campaign_col_idx, subject_col_idx):
                row_campaign = row[campaign_col_idx].strip() if campaign_col_idx < len(row) else ''
                row_subject = row[subject_col_idx].strip() if subject_col_idx < len(row) else ''
                if row_campaign == campaign_name and row_subject == email_subject:
                    row_to_delete = idx
                    break
        
        if row_to_delete:
            # Delete the row (gspread uses 1-indexed rows)
            worksheet.delete_rows(row_to_delete)
            # Clear the cache
            fetch_templates.clear()
            return True
        else:
            return False
            
    except Exception as e:
        st.error(f"Error deleting template: {e}")
        return False

def sync_campaigns(force=False):
    """Fetches all worksheet names and saves them to session state."""
    if not force and st.session_state.get('campaign_list') is not None:
        return
    
    try:
        # Clear Streamlit's internal cache to force a fresh look at the sheet
        st.cache_data.clear()
        
        gc = get_gspread_client()
        if not gc:
            return
        
        sh = gc.open_by_key(SHEET_ID)
        
        # Get all tabs
        worksheet_titles = [ws.title for ws in sh.worksheets()]
        
        # Filter out (DO NOT USE)
        filtered_titles = [
            title for title in worksheet_titles 
            if '(DO NOT USE)' not in title.upper()
        ]
        
        st.session_state.campaign_list = sorted(filtered_titles)
        st.session_state.campaign_data_cache.clear()
        st.sidebar.success(f"Synced {len(filtered_titles)} campaigns!")
        
    except Exception as e:
        st.error(f"Error syncing with Google Sheets: {e}")

def create_campaign(campaign_name: str) -> bool:
    """Create a new campaign worksheet (tab) in the Google Sheet."""
    if not campaign_name:
        st.warning("Please enter a campaign name.")
        return False
    try:
        # 1. Get credentials using the Cloud-Ready logic
        creds = get_google_creds()
        if not creds:
            st.error("Authentication credentials not found.")
            return False
            
        # 2. Authorize gspread
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        
        # 3. Check if worksheet already exists
        titles = [ws.title for ws in sh.worksheets()]
        if campaign_name in titles:
            st.info(f"Campaign '{campaign_name}' already exists.")
            return True
        
        # 4. Create new worksheet
        # Note: rows and cols must be strings or integers depending on gspread version; 
        # using integers is standard.
        worksheet = sh.add_worksheet(title=campaign_name, rows=100, cols=20)
        
        # 5. Initialize with standard headers
        headers = ['Campaign Name', 'Date', 'Platform', 'Name', 'Followers', 'Profile Link', 'Email']
        worksheet.append_row(headers)
        
        # 6. Clear cache and sync
        st.cache_data.clear()
        if 'sync_campaigns' in globals():
            sync_campaigns(force=True)
        
        st.success(f"Successfully created campaign: {campaign_name}")
        return True

    except Exception as e:
        # This will now catch the specific 'st.secrets' error and display it clearly
        st.error(f"Error creating campaign: {e}")
        return False

def send_bulk_emails(selected_rows: List[Dict[str, Any]], html_template: str, subject: str):
    # 1. CLEAN THE INPUTS
    subject = str(subject).replace('\xa0', ' ').strip()
    html_template = str(html_template).replace('\xa0', ' ')
    
    # 2. CLEAN THE SECRETS (In case they were copy-pasted into the box)
    smtp_secrets = st.secrets.get('smtp', {})
    smtp_server = str(smtp_secrets.get('SMTP_SERVER', 'smtp.gmail.com')).replace('\xa0', ' ').strip()
    smtp_port = int(smtp_secrets.get('SMTP_PORT', 587))
    sender_email = str(smtp_secrets.get('SENDER_EMAIL', '')).replace('\xa0', ' ').strip()
    app_password = str(smtp_secrets.get('APP_PASSWORD', '')).replace('\xa0', ' ').strip()

    if not all([smtp_server, sender_email, app_password]):
        return 0, len(selected_rows), ["SMTP credentials missing"]

    success_count = 0
    failed_count = 0
    errors = []
    
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Establish connection once
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, app_password)

            for idx, row in enumerate(selected_rows):
                try:
                    # 3. CLEAN DATA FROM THE SHEET
                    raw_email = row.get('Email', '') or row.get('email', '')
                    raw_name = row.get('Name', '') or row.get('name', 'Partner')
                    
                    # Force conversion to string and scrub
                    clean_email = str(raw_email).replace('\xa0', ' ').strip()
                    clean_name = str(raw_name).replace('\xa0', ' ').strip()

                    if not clean_email or "@" not in clean_email:
                        failed_count += 1
                        errors.append(f"Row {idx+1}: Invalid email '{clean_email}'")
                        continue

                    # 4. Personalize and Build Message
                    p_html = html_template.replace('{name}', clean_name).replace('{Name}', clean_name)
                    
                    msg = MIMEMultipart('alternative')
                    # Use Header for the subject to be 100% safe
                    msg['Subject'] = Header(subject, 'utf-8')
                    msg['From'] = "affiliate@iwellus.com"
                    msg['To'] = clean_email
                    
                    # Attach content with explicit UTF-8 encoding
                    msg.attach(MIMEText(p_html, 'html', 'utf-8'))

                    # 5. SEND AS STRING
                    server.sendmail("affiliate@iwellus.com", clean_email, msg.as_string())
                    
                    success_count += 1
                    st.toast(f"✅ Sent to {clean_name}")
                    
                except Exception as e:
                    failed_count += 1
                    errors.append(f"Row {idx+1} ({clean_name}): {str(e)}")

                progress_bar.progress((idx + 1) / len(selected_rows))
                status_text.text(f"Processing {idx + 1}/{len(selected_rows)}...")

                if idx < len(selected_rows) - 1:
                    time.sleep(random.uniform(3, 7))

    except Exception as e:
        return 0, len(selected_rows), [f"Critical SMTP Error: {str(e)}"]

    progress_bar.empty()
    status_text.empty()
    return success_count, failed_count, errors

def save_to_gsheet(influencer_data: Dict[str, Any], campaign_name: str):
    """Save influencer data to Google Sheet in a campaign-specific tab."""
    try:
        gc = get_gspread_client()
        if not gc:
            return False
        
        sh = gc.open_by_key(SHEET_ID)
        
        # Check if campaign tab exists, if not create it
        try:
            worksheet = sh.worksheet(campaign_name)
        except Exception:
            # Tab doesn't exist, create it
            worksheet = sh.add_worksheet(title=campaign_name, rows=100, cols=20)
            # Add header row to the new tab
            headers = ['Campaign Name', 'Date', 'Platform', 'Name', 'Followers', 'Profile Link', 'Bio', 'Email']
            worksheet.append_row(headers)
        
        author_meta = influencer_data.get('authorMeta', {})
        recent_videos = influencer_data.get('recent_videos', [])
        
        # Extract influencer data
        name = author_meta.get('name', '')
        handle = author_meta.get('nickName') or author_meta.get('name', '')
        followers = author_meta.get('fans', 0)
        signature = author_meta.get('signature', '')
        verified = author_meta.get('verified', False)
        avatar = author_meta.get('avatar', '')
        # Use profileUrl from authorMeta if available, otherwise construct from handle
        profile_url = author_meta.get('profileUrl', '') or (f"https://www.tiktok.com/@{handle}" if handle else '')
        
        # Extract bio from signature
        bio = signature
        
        # Email catching: search bio for email address using regex
        email = ''
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        email_match = re.search(email_pattern, bio)
        if email_match:
            email = email_match.group(0)
        
        # Get video data if available
        video_data = {}
        if recent_videos and len(recent_videos) > 0:
            video = recent_videos[0]
            video_meta = video.get('videoMeta', {})
            video_data = {
                'video_url': video.get('webVideoUrl', '') or video_meta.get('webVideoUrl', ''),
                'cover_url': video_meta.get('coverUrl', '') or video.get('coverUrl', ''),
                'views': video.get('playCount', 0) or video_meta.get('playCount', 0),
                'likes': video.get('diggCount', 0) or video_meta.get('diggCount', 0),
                'comments': video.get('commentCount', 0) or video_meta.get('commentCount', 0)
            }
        
        # Get headers from the worksheet to determine column order
        existing_headers = worksheet.get_all_values()
        if not existing_headers or len(existing_headers) == 0:
            # If no headers exist, use default headers
            headers = ['Campaign Name', 'Date', 'Platform', 'Name', 'Followers', 'Profile Link', 'Bio', 'Email']
            worksheet.append_row(headers)
            existing_headers = [headers]
        
        headers = existing_headers[0]
        
        # Prepare row data based on headers
        row_data = []
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        for header in headers:
            header_lower = header.lower()
            if 'campaign' in header_lower or 'campaign name' in header_lower:
                row_data.append(campaign_name)
            elif 'date' in header_lower:
                row_data.append(current_date)
            elif 'platform' in header_lower:
                row_data.append('TikTok')
            elif 'name' in header_lower and 'handle' not in header_lower and 'user' not in header_lower:
                row_data.append(name)
            elif 'handle' in header_lower or 'username' in header_lower:
                row_data.append(handle)
            elif 'followers' in header_lower or 'fans' in header_lower:
                row_data.append(followers)
            elif 'bio' in header_lower:
                row_data.append(bio)
            elif 'signature' in header_lower or 'description' in header_lower:
                row_data.append(bio)
            elif 'verified' in header_lower:
                row_data.append('Yes' if verified else 'No')
            elif 'avatar' in header_lower or 'profile picture' in header_lower:
                row_data.append(avatar)
            elif 'profile' in header_lower and 'url' in header_lower or 'profile link' in header_lower:
                row_data.append(profile_url)
            elif 'email' in header_lower:
                row_data.append(email)  # Email extracted from bio
            elif 'video' in header_lower and 'url' in header_lower:
                row_data.append(video_data.get('video_url', ''))
            elif 'cover' in header_lower or 'thumbnail' in header_lower:
                row_data.append(video_data.get('cover_url', ''))
            elif 'views' in header_lower:
                row_data.append(video_data.get('views', 0))
            elif 'likes' in header_lower:
                row_data.append(video_data.get('likes', 0))
            elif 'comments' in header_lower:
                row_data.append(video_data.get('comments', 0))
            else:
                row_data.append('')
        
        # Append row to the campaign-specific tab
        worksheet.append_row(row_data)
        
        # Mark this campaign for refresh on next view (lazy loading optimization)
        st.session_state.data_needs_refresh.add(campaign_name)
        
        # If a new tab was created, sync campaigns to update session state
        # (sync_campaigns will pick up the new tab name)
        sync_campaigns()
        
        return True
    except Exception as e:
        st.error(f"Error saving to Google Sheet: {e}")
        return False

# Load environment variables
load_dotenv()

# Get API token from st.secrets (Streamlit Cloud) or .env file
APIFY_API_TOKEN = None
try:
    # Try to get from st.secrets first (for Streamlit Cloud)
    APIFY_API_TOKEN = st.secrets.get('APIFY_API_TOKEN', None)
except (AttributeError, KeyError, TypeError):
    # st.secrets might not be available or key doesn't exist
    pass

# Fall back to .env file if not found in st.secrets
if not APIFY_API_TOKEN:
    APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN')

# Page configuration
st.set_page_config(
    page_title="TikTok Influencer Search",
    page_icon="🎯",
    layout="wide"
)

# Initialize navigation state
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'Discover'

# Initialize selected campaign state
if 'selected_campaign' not in st.session_state:
    st.session_state.selected_campaign = None

# Basic CSS styling
st.markdown("""
<style>
    /* Hide Streamlit default elements */
    #MainMenu {visibility: visible;}
    footer {visibility: hidden;}
    header {visibility: visible;}
    
    /* Style delete buttons with red color */
    div[data-testid*="delete_selected"] button,
    button[data-testid*="delete_selected"] {
        background-color: #dc3545 !important;
        border-color: #dc3545 !important;
        color: white !important;
    }
    div[data-testid*="delete_selected"] button:hover,
    button[data-testid*="delete_selected"]:hover {
        background-color: #c82333 !important;
        border-color: #bd2130 !important;
    }
</style>
""", unsafe_allow_html=True)

# Initialize campaign list state (on-demand loading)
if 'campaign_list' not in st.session_state:
    st.session_state.campaign_list = None

# Initialize campaign data cache for lazy loading
if 'campaign_data_cache' not in st.session_state:
    st.session_state.campaign_data_cache = {}

# Initialize data refresh tracking set
if 'data_needs_refresh' not in st.session_state:
    st.session_state.data_needs_refresh = set()

# Initialize Apify client
if not APIFY_API_TOKEN:
    st.error("❌ APIFY_API_TOKEN not found in st.secrets or .env file. Please add your API token to either source.")
    st.stop()

client = ApifyClient(APIFY_API_TOKEN)

# Filter Dialog Function
@st.dialog("Search Filters")
def filter_dialog():
    """Dialog for search filters."""
    
    hashtag = st.text_input(
        "Hashtag(s)",
        value=st.session_state.get('hashtag', ''),
        placeholder="e.g., hairtok, makeup, beauty",
        help="Enter one or more TikTok hashtags separated by commas (without #)"
    )
    
    results_limit = st.number_input(
        "Results Limit",
        min_value=1,
        max_value=3,
        value=3,
        step=1,
        help="Demo: search is limited to 3 results to save API costs"
    )

    st.subheader("Location")
    location_filter = st.selectbox(
        "Location",
        options=["All", "USA"],
        index=1 if st.session_state.get('location_filter', 'USA') == 'USA' else 0
    )
    
    st.subheader("Follower Range")
    
    # Define follower range options (0 = no min, 999999999 = no max)
    NO_MIN = 0
    NO_MAX = 999999999
    
    min_follower_options = [NO_MIN] + [
        1000, 3000, 5000, 10000, 15000, 20000, 25000, 35000, 50000, 75000,
        100000, 125000, 150000, 175000, 200000, 250000, 300000, 350000,
        500000, 1000000, 2000000, 3000000
    ]
    
    max_follower_options = [
        1000, 3000, 5000, 10000, 15000, 20000, 25000, 35000, 50000, 75000,
        100000, 125000, 150000, 175000, 200000, 250000, 300000, 350000,
        500000, 1000000, 2000000, 3000000
    ] + [NO_MAX]
    
    # Format function to display with k/m suffixes
    def format_followers(value):
        if value == NO_MIN:
            return "No Min"
        elif value == NO_MAX:
            return "No Max"
        elif value >= 1000000:
            return f"{value // 1000000}M"
        elif value >= 1000:
            return f"{value // 1000}K"
        else:
            return str(value)
    
    col1, col2 = st.columns(2)
    
    with col1:
        min_followers = st.selectbox(
            "Min",
            options=min_follower_options,
            index=0,  # Default to "No Min"
            format_func=format_followers
        )
    
    with col2:
        max_followers = st.selectbox(
            "Max",
            options=max_follower_options,
            index=len(max_follower_options) - 1,  # Default to "No Max"
            format_func=format_followers
        )
    
    if min_followers > max_followers:
        st.warning("⚠️ Minimum followers cannot be greater than maximum followers")
    
    # Apply Filters button - also triggers search
    if st.button("Search", type="primary", use_container_width=True):
        # Store filter values in session state
        st.session_state.hashtag = hashtag
        st.session_state.results_limit = results_limit
        st.session_state.location_filter = location_filter
        st.session_state.min_followers = min_followers
        st.session_state.max_followers = max_followers
        # Trigger search
        st.session_state.search_button_clicked = True
        st.rerun()

def get_campaign_data(campaign_name):
    """Fetch influencer data from a specific campaign tab in Google Sheet.
    DEPRECATED: Use get_or_fetch_campaign_data() for lazy loading instead."""
    try:
        gc = get_gspread_client()
        if not gc:
            return []
        
        sh = gc.open_by_key(SHEET_ID)
        try:
            worksheet = sh.worksheet(campaign_name)
        except Exception:
            return []
        
        # Get all rows
        all_values = worksheet.get_all_values()
        
        if not all_values or len(all_values) < 2:
            return []
        
        # Get headers
        headers = all_values[0]
        
        # Convert rows to dictionaries
        data = []
        for row in all_values[1:]:
            if any(cell.strip() for cell in row):  # Skip empty rows
                row_dict = {}
                for i, header in enumerate(headers):
                    row_dict[header] = row[i] if i < len(row) else ''
                data.append(row_dict)
        
        return data
    except Exception as e:
        st.error(f"Error fetching campaign data: {e}")
        return []

def get_or_fetch_campaign_data(campaign_name):
    """
    Lazy loading function to get campaign data from cache or fetch from Google Sheets.
    
    Logic:
    - If campaign_name is NOT in cache OR is in data_needs_refresh:
      - Fetch from Google Sheets
      - Save to cache
      - Remove from refresh set
    - Otherwise: Return cached data
    
    Args:
        campaign_name: Name of the campaign tab
        
    Returns:
        List of dictionaries containing campaign row data
    """
    # Check if we need to fetch (not in cache or needs refresh)
    needs_fetch = (
        campaign_name not in st.session_state.campaign_data_cache or
        campaign_name in st.session_state.data_needs_refresh
    )
    
    if needs_fetch:
        try:
            gc = get_gspread_client()
            if not gc:
                return []
            
            sh = gc.open_by_key(SHEET_ID)
            try:
                worksheet = sh.worksheet(campaign_name)
            except Exception:
                # Campaign tab doesn't exist, cache empty result
                st.session_state.campaign_data_cache[campaign_name] = []
                st.session_state.data_needs_refresh.discard(campaign_name)
                return []
            
            # Get all rows
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) < 2:
                # No data, cache empty result
                st.session_state.campaign_data_cache[campaign_name] = []
                st.session_state.data_needs_refresh.discard(campaign_name)
                return []
            
            # Get headers
            headers = all_values[0]
            
            # Convert rows to dictionaries
            data = []
            for row in all_values[1:]:
                if any(cell.strip() for cell in row):  # Skip empty rows
                    row_dict = {}
                    for i, header in enumerate(headers):
                        row_dict[header] = row[i] if i < len(row) else ''
                    data.append(row_dict)
            
            # Cache the data and remove from refresh set
            st.session_state.campaign_data_cache[campaign_name] = data
            st.session_state.data_needs_refresh.discard(campaign_name)
            
            return data
        except Exception as e:
            st.error(f"Error fetching campaign data: {e}")
            return []
    else:
        # Return cached data
        return st.session_state.campaign_data_cache.get(campaign_name, [])

def delete_rows_from_sheet(campaign_name: str, selected_data: List[Dict[str, Any]], all_data: List[Dict[str, Any]]) -> bool:
    """
    Delete selected rows from Google Sheets.
    
    Args:
        campaign_name: Name of the campaign worksheet
        selected_data: List of dictionaries representing selected rows to delete
        all_data: Complete list of all data rows (to find row indices)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        gc = get_gspread_client()
        if not gc:
            return False
        
        sh = gc.open_by_key(SHEET_ID)
        worksheet = sh.worksheet(campaign_name)
        
        # Get all rows from the sheet
        all_values = worksheet.get_all_values()
        
        if not all_values or len(all_values) < 2:
            return False
        
        # Find the row indices in the sheet that match the selected data
        # We need to match rows based on their content since we don't have unique IDs
        rows_to_delete = []
        
        for selected_row in selected_data:
            # Find matching row in all_data to get its index
            for idx, row_data in enumerate(all_data):
                # Match rows by comparing key fields (Name, Email, Profile Link are likely unique)
                if (selected_row.get('Name', '') == row_data.get('Name', '') and
                    selected_row.get('Email', '') == row_data.get('Email', '') and
                    selected_row.get('Profile Link', '') == row_data.get('Profile Link', '')):
                    # Row index in sheet = data index + 2 (header row + 1-indexed)
                    sheet_row_num = idx + 2
                    rows_to_delete.append(sheet_row_num)
                    break
        
        if not rows_to_delete:
            return False
        
        # Sort in descending order to delete from bottom to top (avoid index shifting)
        rows_to_delete.sort(reverse=True)
        
        # Delete rows from bottom to top
        for row_num in rows_to_delete:
            worksheet.delete_rows(row_num)
        
        return True
    except Exception as e:
        st.error(f"Error deleting rows: {e}")
        return False

@st.dialog("Confirm Deletion")
def confirm_deletion_dialog(count: int, campaign_name: str, selected_data: List[Dict[str, Any]], all_data: List[Dict[str, Any]]):
    """
    Confirmation dialog for bulk deletion.
    
    Args:
        count: Number of rows to delete
        campaign_name: Name of the campaign
        selected_data: Selected rows to delete
        all_data: Complete list of all data rows
    """
    st.warning(f"Are you sure you want to permanently delete {count} influencer(s) from this campaign? This action cannot be undone.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    
    with col2:
        if st.button("Yes, Delete Permanently", use_container_width=True, type="primary"):
            if delete_rows_from_sheet(campaign_name, selected_data, all_data):
                # Clear cache to force fresh data fetch
                st.cache_data.clear()
                # Also clear session state cache
                if campaign_name in st.session_state.campaign_data_cache:
                    del st.session_state.campaign_data_cache[campaign_name]
                st.session_state.data_needs_refresh.add(campaign_name)
                
                st.toast("✅ Deleted successfully!")
                st.rerun()
            else:
                st.error("❌ Failed to delete rows. Please try again.")

@st.dialog("Edit Email Template", width="large")
def edit_email_dialog(campaign_name: str, email_subject_key: str, email_template_key: str, dm_template_key: str):
    """
    Dialog for editing email template (subject and HTML).
    
    Args:
        campaign_name: Name of the campaign
        email_subject_key: Session state key for email subject
        email_template_key: Session state key for email template
        dm_template_key: Session state key for DM template (needed for saving)
    """
    
    # Two-column layout
    col1, col2 = st.columns([0.4, 0.6])
    
    with col1:
        # Column 1: Editor
        # Email Subject input
        email_subject = st.text_input(
            "Email Subject",
            value=st.session_state.get(email_subject_key, f"Partnership Opportunity - {campaign_name}"),
            key=f"email_subject_dialog_{campaign_name}",
            help="Enter the email subject line"
        )
        
        # HTML Template text area
        email_template = st.text_area(
            "Email Template (HTML)",
            value=st.session_state.get(email_template_key, ''),
            key=f"email_template_dialog_{campaign_name}",
            height=400,
            help="Enter HTML code for your email template"
        )
    
    with col2:
        # Column 2: Live Preview
        st.caption("Preview")
        
        # Render HTML preview with style block to prevent image cropping
        if email_template:
            preview_html = f"""
            <head>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                html, body {{
                    margin: 0;
                    padding: 0;
                    width: 100%;
                    overflow-x: hidden;
                }}
                img {{
                    max-width: 100% !important;
                    height: auto !important;
                }}
                .preview-wrapper {{
                    width: 100%;
                    box-sizing: border-box;
                    padding: 10px;
                }}
            </style>
        </head>
        <div class="preview-wrapper">
                {email_template}
            </div>
            """
            st.components.v1.html(preview_html, height=500, width=None, scrolling=True)
        else:
            st.info("Enter HTML content to see preview")
            empty_preview_html = """
            <style>
                img {{
                    max-width: 100% !important;
                    height: auto !important;
                    display: block;
                    margin: 0 auto;
                }}
                body {{
                    margin: 0;
                    padding: 10px;
                    font-family: sans-serif;
                    overflow-x: hidden;
                }}
            </style>
            <div style="width: 100%; box-sizing: border-box;">
                No content to preview
            </div>
            """
            st.components.v1.html(empty_preview_html, height=500, width=None, scrolling=True)
    
    # Footer: Buttons at the bottom (outside columns)
    st.divider()
    footer_col1, footer_col2 = st.columns([1, 1])
    
    with footer_col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    
    with footer_col2:
        if st.button("Save", use_container_width=True, type="primary"):
            # Update session state
            st.session_state[email_subject_key] = email_subject
            st.session_state['email_subject'] = email_subject  # Also maintain global key
            st.session_state[email_template_key] = email_template
            
            # Get current DM template value
            current_dm = st.session_state.get(dm_template_key, '')
            
            # Save to Google Sheets
            if save_template_to_sheet(campaign_name, email_subject, email_template, current_dm):
                st.success("✅ Template saved successfully!")
                st.rerun()
            else:
                st.error("❌ Failed to save template. Please try again.")

@st.dialog("Edit Global Email Template", width="large")
def edit_global_email_dialog(template_subject: str = None):
    """
    Dialog for editing Global email templates.
    If template_subject is None, creates a new template.
    If template_subject is provided, edits the template with that subject.
    """
    global_templates = get_global_templates()
    
    if template_subject:
        # Editing existing template - find by subject
        template = None
        for t in global_templates:
            if t.get('subject', '') == template_subject:
                template = t
                break
        
        if template:
            default_subject = template.get('subject', '')
            default_html = template.get('email', '')
            old_subject = default_subject
        else:
            # Template not found, create new
            default_subject = ''
            default_html = ''
            old_subject = None
    else:
        # Creating new template
        default_subject = ''
        default_html = ''
        old_subject = None
    
    col1, col2 = st.columns([0.4, 0.6])
    
    with col1:
        email_subject = st.text_input(
            "Email Subject",
            value=default_subject,
            key=f"global_email_subject_{template_subject or 'new'}",
            help="Enter the email subject line"
        )
        
        email_template = st.text_area(
            "Email Template (HTML)",
            value=default_html,
            key=f"global_email_template_{template_subject or 'new'}",
            height=400,
            help="Enter HTML code for your email template"
        )
    
    with col2:
        st.caption("Preview")
        if email_template:
            preview_html = f"""
            <head>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                html, body {{
                    margin: 0;
                    padding: 0;
                    width: 100%;
                    overflow-x: hidden;
                }}
                img {{
                    max-width: 100% !important;
                    height: auto !important;
                }}
                .preview-wrapper {{
                    width: 100%;
                    box-sizing: border-box;
                    padding: 10px;
                }}
            </style>
        </head>
        <div class="preview-wrapper">
                {email_template}
            </div>
            """
            st.components.v1.html(preview_html, height=500, width=None, scrolling=True)
        else:
            st.info("Enter HTML content to see preview")
    
    st.divider()
    footer_col1, footer_col2 = st.columns([1, 1])
    
    with footer_col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    
    with footer_col2:
        if st.button("Save", use_container_width=True, type="primary"):
            if not email_subject.strip():
                st.error("Please enter an email subject.")
            else:
                # Save with Campaign Name = "Global"
                if save_template_to_sheet("Global", email_subject, email_template, '', old_subject=old_subject):
                    st.success("✅ Template saved successfully!")
                    st.rerun()
                else:
                    st.error("❌ Failed to save template. Please try again.")

@st.dialog("Edit Global DM Template", width="small")
def edit_global_dm_dialog(template_subject: str = None):
    """
    Dialog for editing Global DM templates.
    If template_subject is None, creates a new template.
    If template_subject is provided, edits the template with that subject.
    """
    global_templates = get_global_templates()
    
    if template_subject:
        # Editing existing template - find by subject
        template = None
        for t in global_templates:
            if t.get('subject', '') == template_subject:
                template = t
                break
        
        if template:
            default_dm = template.get('dm', '')
            default_subject = template.get('subject', '')
            old_subject = default_subject
        else:
            # Template not found, create new
            default_dm = ''
            default_subject = ''
            old_subject = None
    else:
        # Creating new template
        default_dm = ''
        default_subject = ''
        old_subject = None
    
    dm_subject = st.text_input(
        "Template Name",
        value=default_subject,
        key=f"global_dm_subject_{template_subject or 'new'}",
        help="Enter a name for this DM template"
    )
    
    dm_template = st.text_area(
        "DM Template (Text)",
        value=default_dm,
        key=f"global_dm_template_{template_subject or 'new'}",
        height=400,
        help="Enter plain text for your DM template (line breaks are preserved)"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    
    with col2:
        if st.button("Save", use_container_width=True, type="primary"):
            if not dm_subject.strip():
                st.error("Please enter a template name.")
            else:
                # Save with Campaign Name = "Global", empty HTML template
                if save_template_to_sheet("Global", dm_subject, '', dm_template, old_subject=old_subject):
                    st.success("✅ Template saved successfully!")
                    st.rerun()
                else:
                    st.error("❌ Failed to save template. Please try again.")

def render_templates_page():
    """Render the Templates sidebar page for Global templates."""
    st.subheader("Templates")
    st.caption("Manage Global templates (Campaign Name = 'Global')")
    
    tab1, tab2 = st.tabs(["Email", "DM"])
    
    with tab1:
      
        
        all_global_templates = get_global_templates()
        # Filter to only show templates with HTML content
        global_email_templates = [t for t in all_global_templates if t.get('email', '').strip()]
        
        if st.button("➕ Add New Email Template", type="primary", use_container_width=True):
            edit_global_email_dialog(None)
        
        if global_email_templates:
            st.divider()
            for idx, template in enumerate(global_email_templates):
                subject = template.get('subject', 'Untitled')
                html_content = template.get('email', '')
                
                with st.expander(f"📧 {subject}", expanded=False):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.markdown(f"**Subject:** {subject}")
                        if html_content:
                            st.caption(f"HTML: {len(html_content)} characters")
                        else:
                            st.caption("No HTML content")
                    
                    with col2:
                        edit_col, delete_col = st.columns(2)
                        with edit_col:
                            if st.button("Edit", key=f"edit_email_{idx}", use_container_width=True):
                                edit_global_email_dialog(subject)
                        with delete_col:
                            if st.button("Delete", key=f"delete_email_{idx}", use_container_width=True, type="primary"):
                                if delete_template_from_sheet("Global", subject):
                                    st.success("✅ Template deleted!")
                                    st.rerun()
                                else:
                                    st.error("❌ Failed to delete template.")
        else:
            st.info("No email templates found. Click 'Add New Email Template' to create one.")
    
    with tab2:
        
        
        global_dm_templates = get_global_templates()
        
        if st.button("➕ Add New DM Template", type="primary", use_container_width=True):
            edit_global_dm_dialog(None)
        
        if global_dm_templates:
            st.divider()
            for idx, template in enumerate(global_dm_templates):
                subject = template.get('subject', 'Untitled')
                dm_content = template.get('dm', '')
                
                # Only show templates that have DM content
                if dm_content.strip():
                    with st.expander(f"💬 {subject}", expanded=False):
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            st.markdown(f"**Template Name:** {subject}")
                            if dm_content:
                                st.caption(f"Text: {len(dm_content)} characters")
                                with st.expander("Preview"):
                                    st.text(dm_content)
                        
                        with col2:
                            edit_col, delete_col = st.columns(2)
                            with edit_col:
                                if st.button("Edit", key=f"edit_dm_{idx}", use_container_width=True):
                                    edit_global_dm_dialog(subject)
                            with delete_col:
                                if st.button("Delete", key=f"delete_dm_{idx}", use_container_width=True, type="primary"):
                                    if delete_template_from_sheet("Global", subject):
                                        st.success("✅ Template deleted!")
                                        st.rerun()
                                    else:
                                        st.error("❌ Failed to delete template.")
        else:
            st.info("No DM templates found. Click 'Add New DM Template' to create one.")

@st.dialog("Send Bulk Email", width="large")
def send_bulk_email_dialog(selected_data: List[Dict[str, Any]], campaign_name: str):
    """
    Dialog for sending bulk emails with template selection.
    
    Args:
        selected_data: List of selected influencer rows
        campaign_name: Name of the current campaign
    """
    # Two-column layout
    col1, col2 = st.columns([0.4, 0.6])
    
    with col1:
        # Template source selection
        template_source = st.selectbox(
            "Template Source",
            options=["Global Template", "Internal Template"],
            index=0,  # Default to Internal Template
            key="template_source_select"
        )
        
        # Get templates based on selection
        if template_source == "Internal Template":
            templates = get_templates_by_campaign(campaign_name)
            # Filter to only email templates (those with HTML content)
            email_templates = [t for t in templates if t.get('email', '').strip()]
        else:  # Global Template
            email_templates = [t for t in get_global_templates() if t.get('email', '').strip()]
        
        if not email_templates:
            st.warning(f"No email templates found for {template_source.lower()}.")
            if st.button("Close", use_container_width=True):
                st.rerun()
            return
        
        # Radio buttons for template selection (using Email Subject as the identifier)
        template_options = [t.get('subject', 'Untitled') for t in email_templates]
        selected_template_name = st.radio(
            "Select Template",
            options=template_options,
            key="template_radio_select"
        )
        
        # Find the selected template
        selected_template = None
        for t in email_templates:
            if t.get('subject', 'Untitled') == selected_template_name:
                selected_template = t
                break
    
    with col2:
        # Preview section
        st.caption("Preview")
        
        if selected_template:
            st.markdown(f"**Subject:** {selected_template.get('subject', '')}")
            
            html_body = selected_template.get('email', '')
            if html_body:
                preview_html = f"""
                <head>
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <style>
                    html, body {{
                        margin: 0;
                        padding: 10px;
                        width: 100%;
                        overflow-x: hidden;
                    }}
                    img {{
                        max-width: 100% !important;
                        height: auto !important;
                    }}
                </style>
            </head>
            <div>
                    {html_body}
                </div>
                """
                st.components.v1.html(preview_html, height=400, width=None, scrolling=True)
            else:
                st.warning("Template has no HTML content.")
        else:
            st.info("Select a template to see preview")
    
    # Confirm & Send button (outside columns, full width)
    st.divider()
    button_col1, button_col2 = st.columns([1, 1])
    
    with button_col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    
    with button_col2:
        if selected_template and st.button("Confirm & Send", use_container_width=True, type="primary"):
            # Execute send_bulk_emails
            html_body = selected_template.get('email', '')
            success_count, failed_count, errors = send_bulk_emails(
                selected_data,
                html_body,
                selected_template.get('subject', '')
            )
            
            if failed_count == 0:
                st.success(f"✅ Successfully sent {success_count} email(s)!")
                time.sleep(1)
                st.rerun()
            else:
                st.warning(f"⚠️ Sent {success_count} email(s), {failed_count} failed.")
                if errors:
                    with st.expander("View Errors"):
                        for error in errors:
                            st.text(error)

@st.dialog("Copy DM", width="medium")
def copy_dm_dialog(selected_data: List[Dict[str, Any]], campaign_name: str):
    """
    Dialog for copying DM templates with template selection and preview.
    
    Args:
        selected_data: List of selected influencer rows
        campaign_name: Name of the current campaign
    """
    # Two-column layout
    col1, col2 = st.columns([1, 1])
    
    with col1:
        # Template source selection
        template_source = st.selectbox(
            "Template Source",
            options=["Global Template", "Internal Template"],
            index=0,  # Default to Internal Template
            key="copy_dm_template_source_select"
        )
        
        # Get templates based on selection
        if template_source == "Internal Template":
            templates = get_templates_by_campaign(campaign_name)
            # Filter to only DM templates (those with DM content)
            dm_templates = [t for t in templates if t.get('dm', '').strip()]
            # For Internal Templates, default to first template (index=0)
            default_radio_index = 0 if dm_templates else None
        else:  # Global Template
            dm_templates = [t for t in get_global_templates() if t.get('dm', '').strip()]
            # For Global Templates, if exactly 1 template, set index=None to force user selection
            default_radio_index = None if len(dm_templates) == 1 else (0 if dm_templates else None)
        
        if not dm_templates:
            st.warning(f"No DM templates found for {template_source.lower()}.")
            if st.button("Close", use_container_width=True):
                st.rerun()
            return
        
        # Radio buttons for template selection (using subject/name as the identifier)
        template_options = [t.get('subject', 'Untitled') for t in dm_templates]
        selected_template_name = st.radio(
            "Select Template",
            options=template_options,
            index=default_radio_index,
            key="copy_dm_template_radio_select"
        )
        
        # Find the selected template only if a template is selected
        selected_template = None
        if selected_template_name is not None:
            for t in dm_templates:
                if t.get('subject', 'Untitled') == selected_template_name:
                    selected_template = t
                    break
    
    with col2:
        # Preview section
        st.caption("Preview")
        
        if selected_template_name is not None and selected_template:
            st.markdown(f"{selected_template.get('subject', '')}")
            
            dm_body = selected_template.get('dm', '')
            if dm_body:
                # Display DM text using st.code for one-click copy functionality
                st.code(dm_body, language=None)
            else:
                st.warning("Template has no DM content.")
        else:
            st.info("Please select a template from the list above to preview.")
    
    

@st.dialog("Edit DM Template", width="small")
def edit_dm_dialog(campaign_name: str, email_subject_key: str, email_template_key: str, dm_template_key: str):
    """
    Dialog for editing DM template.
    
    Args:
        campaign_name: Name of the campaign
        email_subject_key: Session state key for email subject (needed for saving)
        email_template_key: Session state key for email template (needed for saving)
        dm_template_key: Session state key for DM template
    """
    
    # DM Template text area
    dm_template = st.text_area(
        "DM Template (Text)",
        value=st.session_state.get(dm_template_key, ''),
        key=f"dm_template_dialog_{campaign_name}",
        height=400,
        help="Enter plain text for your DM template (line breaks are preserved)"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    
    with col2:
        if st.button("Save", use_container_width=True, type="primary"):
            # Update session state
            st.session_state[dm_template_key] = dm_template
            
            # Get current email subject and template values
            current_subject = st.session_state.get(email_subject_key, f"Partnership Opportunity - {campaign_name}")
            current_email = st.session_state.get(email_template_key, '')
            
            # Save to Google Sheets
            if save_template_to_sheet(campaign_name, current_subject, current_email, dm_template):
                st.success("✅ DM Template saved successfully!")
                st.rerun()
            else:
                st.error("❌ Failed to save DM template. Please try again.")

# Campaign Page Function
def render_campaign_page():
    """Render the Campaign page."""
    
    # Check if a campaign is selected
    if st.session_state.selected_campaign:
        # Show selected campaign view
        campaign_name = st.session_state.selected_campaign
        
        # Back button
        if st.button("ᐸ Back"):
            st.session_state.selected_campaign = None
            st.rerun()
        
        st.subheader(f"{campaign_name}")
        
        # Initialize template session state if not exists
        email_template_key = f"email_template_{campaign_name}"
        dm_template_key = f"dm_template_{campaign_name}"
        email_subject_key = f"email_subject_{campaign_name}"
        
        # Load templates from Google Sheets if not in session state
        # For backward compatibility, load the first template if multiple exist
        if email_template_key not in st.session_state or dm_template_key not in st.session_state or email_subject_key not in st.session_state:
            campaign_templates_list = get_templates_by_campaign(campaign_name)
            # Use first template if available, otherwise use empty defaults
            if campaign_templates_list and len(campaign_templates_list) > 0:
                campaign_template = campaign_templates_list[0]
            else:
                campaign_template = {}
            # Load email subject for this campaign if it exists
            if email_subject_key not in st.session_state:
                subject_value = campaign_template.get('subject', f"Partnership Opportunity - {campaign_name}")
                st.session_state[email_subject_key] = subject_value
                st.session_state['email_subject'] = subject_value  # Also maintain global key
            # Load email template for this campaign if it exists
            if email_template_key not in st.session_state:
                st.session_state[email_template_key] = campaign_template.get('email', '')
            # Load DM template for this campaign if it exists
            if dm_template_key not in st.session_state:
                st.session_state[dm_template_key] = campaign_template.get('dm', '')
        
        # Lazy load campaign data (from cache or fetch if needed)
        campaign_data = get_or_fetch_campaign_data(campaign_name)
        
        if campaign_data:
            # Filtering Logic: Split campaign_data into email_list and dm_list
            email_list = []
            dm_list = []
            
            for row in campaign_data:
                # Check if Email column exists and is not empty
                email_value = row.get('Email', '') or row.get('email', '')
                if email_value and str(email_value).strip():
                    email_list.append(row)
                else:
                    dm_list.append(row)
            
            # Template Section: Add two buttons above the tabs
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button(
                    "Edit Email Template",
                    key=f"edit_email_btn_{campaign_name}",
                    use_container_width=True,
                    type="secondary"
                ):
                    edit_email_dialog(campaign_name, email_subject_key, email_template_key, dm_template_key)
            
            with col2:
                if st.button(
                    "Edit DM Template",
                    key=f"edit_dm_btn_{campaign_name}",
                    use_container_width=True,
                    type="secondary"
                ):
                    edit_dm_dialog(campaign_name, email_subject_key, email_template_key, dm_template_key)
            
            # 3-tab layout with row counts
            tab1, tab2, tab3 = st.tabs([
                f"All ({len(campaign_data)})",
                f"Email ({len(email_list)})",
                f"DM ({len(dm_list)})"
            ])
            
            # Column configuration for dataframes
            column_config = {
                "Select": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select rows for bulk actions",
                    width="small"
                ),
                "Profile Link": st.column_config.LinkColumn(
                    "Profile Link",
                    help="Click to open TikTok profile"
                ),
                "Followers": st.column_config.NumberColumn(
                    "Followers",
                    format="%d"
                )
            }
            
            with tab1:
                # All tab - displays complete campaign data
                if campaign_data:
                    # Create placeholder container at the very beginning
                    action_bar_placeholder = st.container()
                    
                    # Setup editor key
                    editor_key_all = f"all_editor_{campaign_name}"
                    
                    # Handle Select All toggle
                    select_all_key = f"select_all_all_{campaign_name}"
                    if select_all_key not in st.session_state:
                        st.session_state[select_all_key] = False
                    
                    # Prepare base data with Select column at index 0
                    base_data_with_select = []
                    for row in campaign_data:
                        row_copy = row.copy()
                        # Remove Campaign Name column
                        row_copy.pop('Campaign Name', None)
                        # Initialize Select based on select_all state
                        select_value = st.session_state[select_all_key]
                        row_ordered = {'Select': select_value}
                        row_ordered.update({k: v for k, v in row_copy.items() if k != 'Select'})
                        base_data_with_select.append(row_ordered)
                    
                    # Get previous edited data (Streamlit manages this automatically)
                    previous_edited = st.session_state.get(editor_key_all, base_data_with_select)
                    
                    # Convert to list if needed and remove Campaign Name column
                    if isinstance(previous_edited, pd.DataFrame):
                        # Remove Campaign Name column if it exists
                        if 'Campaign Name' in previous_edited.columns:
                            previous_edited = previous_edited.drop(columns=['Campaign Name'])
                        data_for_editor = previous_edited.to_dict('records')
                    elif isinstance(previous_edited, list):
                        # Remove Campaign Name from each row
                        data_for_editor = []
                        for row in previous_edited:
                            row_copy = row.copy()
                            row_copy.pop('Campaign Name', None)
                            data_for_editor.append(row_copy)
                    else:
                        data_for_editor = base_data_with_select
                    
                    # Render the table
                    edited_df = st.data_editor(
                        data_for_editor,
                        use_container_width=True,
                        hide_index=True,
                        column_config=column_config,
                        key=editor_key_all
                    )
                    
                    # Convert edited_df to DataFrame if it's a list
                    if isinstance(edited_df, list):
                        edited_df = pd.DataFrame(edited_df)
                    elif not isinstance(edited_df, pd.DataFrame):
                        edited_df = pd.DataFrame(data_for_editor)
                    
                    # Ensure Select column exists
                    if 'Select' not in edited_df.columns:
                        edited_df['Select'] = False
                    
                    # Calculate selection AFTER rendering
                    selected_rows = edited_df[edited_df["Select"] == True] if "Select" in edited_df.columns else pd.DataFrame()
                    
                    # Backfill the placeholder with action buttons
                    with action_bar_placeholder:
                        col1, col2, col3, spacer,col4 = st.columns([2, 3, 4, 10, 4])
                        
                        with col1:
                            # Select All button
                            all_selected = len(selected_rows) == len(edited_df) and len(edited_df) > 0
                            select_all_label = "Deselect All" if all_selected else "Select All"
                            if st.button(select_all_label, key=f"select_all_btn_all_{campaign_name}", type="tertiary", use_container_width=True):
                                # Toggle select all state
                                st.session_state[select_all_key] = not st.session_state[select_all_key]
                                st.rerun()
                        
                        with col2:
                            # Copy DM button - always shown
                            if not selected_rows.empty:
                                selected_data_all = []
                                for idx, row in selected_rows.iterrows():
                                    row_dict = row.to_dict()
                                    row_dict.pop('Select', None)
                                    selected_data_all.append(row_dict)
                            else:
                                selected_data_all = []
                            
                            if st.button(
                                "Copy DM",
                                key=f"copy_dm_all_{campaign_name}",
                                use_container_width=True,
                                type="primary"
                            ):
                                copy_dm_dialog(selected_data_all, campaign_name)
                        
                        with col3:
                            # Send Bulk Email button - only show if rows are selected and have emails
                            if not selected_rows.empty:
                                # Get selected data (remove Select column)
                                selected_data_all = []
                                for idx, row in selected_rows.iterrows():
                                    row_dict = row.to_dict()
                                    row_dict.pop('Select', None)
                                    selected_data_all.append(row_dict)
                                
                                # Check if ALL selected influencers have non-empty Email values
                                all_have_emails = True
                                if selected_data_all:
                                    for row in selected_data_all:
                                        email_value = row.get('Email', '') or row.get('email', '')
                                        if not email_value or (isinstance(email_value, str) and not email_value.strip()):
                                            all_have_emails = False
                                            break
                                
                                selected_count = len(selected_rows)
                                
                                # Show "Send Bulk Email" only if all have emails
                                if all_have_emails and selected_data_all:
                                    if st.button(
                                        f"Send Bulk Email ({selected_count})",
                                        key=f"send_bulk_email_all_{campaign_name}",
                                        use_container_width=True,
                                        type="primary"
                                    ):
                                        send_bulk_email_dialog(selected_data_all, campaign_name)
                        
                        with col4:
                            # Delete button - only show if rows are selected
                            if not selected_rows.empty:
                                selected_data_all = []
                                for idx, row in selected_rows.iterrows():
                                    row_dict = row.to_dict()
                                    row_dict.pop('Select', None)
                                    selected_data_all.append(row_dict)
                                
                                selected_count = len(selected_rows)
                                if st.button(
                                    f"Delete Selected ({selected_count})",
                                    key=f"delete_selected_all_{campaign_name}",
                                    use_container_width=True,
                                    type="primary"
                                ):
                                    if selected_data_all:
                                        confirm_deletion_dialog(len(selected_data_all), campaign_name, selected_data_all, campaign_data)
                else:
                    st.info("No influencers found in this campaign.")
            
            with tab2:
                if email_list:
                    # Create placeholder container at the very beginning
                    action_bar_placeholder = st.container()
                    
                    # Setup editor key
                    editor_key_email = f"email_editor_{campaign_name}"
                    
                    # Handle Select All toggle
                    select_all_key = f"select_all_email_{campaign_name}"
                    if select_all_key not in st.session_state:
                        st.session_state[select_all_key] = False
                    
                    # Prepare base data with Select column at index 0
                    base_data_with_select = []
                    for row in email_list:
                        row_copy = row.copy()
                        # Remove Campaign Name column
                        row_copy.pop('Campaign Name', None)
                        # Initialize Select based on select_all state
                        select_value = st.session_state[select_all_key]
                        row_ordered = {'Select': select_value}
                        row_ordered.update({k: v for k, v in row_copy.items() if k != 'Select'})
                        base_data_with_select.append(row_ordered)
                    
                    # Get previous edited data (Streamlit manages this automatically)
                    previous_edited = st.session_state.get(editor_key_email, base_data_with_select)
                    
                    # Convert to list if needed and remove Campaign Name column
                    if isinstance(previous_edited, pd.DataFrame):
                        # Remove Campaign Name column if it exists
                        if 'Campaign Name' in previous_edited.columns:
                            previous_edited = previous_edited.drop(columns=['Campaign Name'])
                        data_for_editor = previous_edited.to_dict('records')
                    elif isinstance(previous_edited, list):
                        # Remove Campaign Name from each row
                        data_for_editor = []
                        for row in previous_edited:
                            row_copy = row.copy()
                            row_copy.pop('Campaign Name', None)
                            data_for_editor.append(row_copy)
                    else:
                        data_for_editor = base_data_with_select
                    
                    # Render the table
                    edited_df = st.data_editor(
                        data_for_editor,
                        use_container_width=True,
                        hide_index=True,
                        column_config=column_config,
                        key=editor_key_email
                    )
                    
                    # Convert edited_df to DataFrame if it's a list
                    if isinstance(edited_df, list):
                        edited_df = pd.DataFrame(edited_df)
                    elif not isinstance(edited_df, pd.DataFrame):
                        edited_df = pd.DataFrame(data_for_editor)
                    
                    # Ensure Select column exists
                    if 'Select' not in edited_df.columns:
                        edited_df['Select'] = False
                    
                    # Calculate selection AFTER rendering
                    selected_rows = edited_df[edited_df["Select"] == True] if "Select" in edited_df.columns else pd.DataFrame()
                    
                    # Backfill the placeholder with action buttons
                    with action_bar_placeholder:
                        col1, col2, spacer, col3 = st.columns([1.5, 3, 10, 3])
                        
                        with col1:
                            # Select All button
                            all_selected = len(selected_rows) == len(edited_df) and len(edited_df) > 0
                            select_all_label = "Deselect All" if all_selected else "Select All"
                            if st.button(select_all_label, key=f"select_all_btn_email_{campaign_name}", type="tertiary", use_container_width=True):
                                # Toggle select all state
                                st.session_state[select_all_key] = not st.session_state[select_all_key]
                                st.rerun()
                        
                        with col2:
                            # Send Bulk Email button - only show if rows are selected
                            if not selected_rows.empty:
                                # Get selected data (remove Select column)
                                selected_data = []
                                for idx, row in selected_rows.iterrows():
                                    row_dict = row.to_dict()
                                    row_dict.pop('Select', None)
                                    selected_data.append(row_dict)
                                
                                # All email_list items have emails by definition
                                selected_count = len(selected_rows)
                                if st.button(
                                    f"Send Bulk Email ({selected_count})",
                                    key=f"send_bulk_email_{campaign_name}",
                                    use_container_width=True,
                                    type="primary"
                                ):
                                    if selected_data:
                                        send_bulk_email_dialog(selected_data, campaign_name)
                        
                        with col3:
                            # Delete button - only show if rows are selected
                            if not selected_rows.empty:
                                selected_data = []
                                for idx, row in selected_rows.iterrows():
                                    row_dict = row.to_dict()
                                    row_dict.pop('Select', None)
                                    selected_data.append(row_dict)
                                
                                selected_count = len(selected_rows)
                                if st.button(
                                    f"Delete Selected ({selected_count})",
                                    key=f"delete_selected_email_{campaign_name}",
                                    use_container_width=True,
                                    type="primary"
                                ):
                                    if selected_data:
                                        confirm_deletion_dialog(len(selected_data), campaign_name, selected_data, campaign_data)
                        
                else:
                    st.info("No influencers with email addresses found.")
            
            with tab3:
                if dm_list:
                    # Create placeholder container at the very beginning
                    action_bar_placeholder = st.container()
                    
                    # Setup editor key
                    editor_key_dm = f"dm_editor_{campaign_name}"
                    
                    # Handle Select All toggle
                    select_all_key = f"select_all_dm_{campaign_name}"
                    if select_all_key not in st.session_state:
                        st.session_state[select_all_key] = False
                    
                    # Prepare base data with Select column at index 0
                    base_data_with_select = []
                    for row in dm_list:
                        row_copy = row.copy()
                        # Remove Campaign Name column
                        row_copy.pop('Campaign Name', None)
                        # Initialize Select based on select_all state
                        select_value = st.session_state[select_all_key]
                        row_ordered = {'Select': select_value}
                        row_ordered.update({k: v for k, v in row_copy.items() if k != 'Select'})
                        base_data_with_select.append(row_ordered)
                    
                    # Get previous edited data (Streamlit manages this automatically)
                    previous_edited = st.session_state.get(editor_key_dm, base_data_with_select)
                    
                    # Convert to list if needed and remove Campaign Name column
                    if isinstance(previous_edited, pd.DataFrame):
                        # Remove Campaign Name column if it exists
                        if 'Campaign Name' in previous_edited.columns:
                            previous_edited = previous_edited.drop(columns=['Campaign Name'])
                        data_for_editor = previous_edited.to_dict('records')
                    elif isinstance(previous_edited, list):
                        # Remove Campaign Name from each row
                        data_for_editor = []
                        for row in previous_edited:
                            row_copy = row.copy()
                            row_copy.pop('Campaign Name', None)
                            data_for_editor.append(row_copy)
                    else:
                        data_for_editor = base_data_with_select
                    
                    # Render the table
                    edited_df = st.data_editor(
                        data_for_editor,
                        use_container_width=True,
                        hide_index=True,
                        column_config=column_config,
                        key=editor_key_dm
                    )
                    
                    # Convert edited_df to DataFrame if it's a list
                    if isinstance(edited_df, list):
                        edited_df = pd.DataFrame(edited_df)
                    elif not isinstance(edited_df, pd.DataFrame):
                        edited_df = pd.DataFrame(data_for_editor)
                    
                    # Ensure Select column exists
                    if 'Select' not in edited_df.columns:
                        edited_df['Select'] = False
                    
                    # Calculate selection AFTER rendering
                    selected_rows = edited_df[edited_df["Select"] == True] if "Select" in edited_df.columns else pd.DataFrame()
                    
                    # Backfill the placeholder with action buttons
                    with action_bar_placeholder:
                        col1, col2, spacer, col3 = st.columns([1.5, 3, 10, 3])
                        
                        with col1:
                            # Select All button
                            all_selected = len(selected_rows) == len(edited_df) and len(edited_df) > 0
                            select_all_label = "Deselect All" if all_selected else "Select All"
                            if st.button(select_all_label, key=f"select_all_btn_dm_{campaign_name}", type="tertiary", use_container_width=True):
                                # Toggle select all state
                                st.session_state[select_all_key] = not st.session_state[select_all_key]
                                st.rerun()
                        
                        with col2:
                            # Copy DM button - always shown
                            if not selected_rows.empty:
                                selected_data_dm = []
                                for idx, row in selected_rows.iterrows():
                                    row_dict = row.to_dict()
                                    row_dict.pop('Select', None)
                                    selected_data_dm.append(row_dict)
                                selected_count = len(selected_rows)
                                button_label = f"Copy DM"
                            else:
                                selected_data_dm = []
                                button_label = "Copy DM"
                            
                            if st.button(
                                button_label,
                                key=f"copy_dm_{campaign_name}",
                                use_container_width=True,
                                type="primary"
                            ):
                                copy_dm_dialog(selected_data_dm, campaign_name)
                        
                        with col3:
                            # Delete button - only show if rows are selected
                            if not selected_rows.empty:
                                selected_data_dm = []
                                for idx, row in selected_rows.iterrows():
                                    row_dict = row.to_dict()
                                    row_dict.pop('Select', None)
                                    selected_data_dm.append(row_dict)
                                
                                selected_count = len(selected_rows)
                                if st.button(
                                    f"Delete Selected ({selected_count})",
                                    key=f"delete_selected_dm_{campaign_name}",
                                    use_container_width=True,
                                    type="primary"
                                ):
                                    if selected_data_dm:
                                        confirm_deletion_dialog(len(selected_data_dm), campaign_name, selected_data_dm, campaign_data)
                    
                else:
                    st.info("No influencers without email addresses found.")
        else:
            st.info("No influencers found in this campaign.")
    else:
        # Show campaign list view
        st.subheader("Your Campaigns") 
        
        # Use session state as primary data source (sync if needed)
        if st.session_state.campaign_list is None:
            sync_campaigns()
        
        campaign_names = st.session_state.campaign_list if st.session_state.campaign_list is not None else []
        
        if campaign_names:
            # Display campaigns as cards/buttons
            for campaign_name in campaign_names:
                # Create a card-like button with professional styling
                if st.button(
                    f"{campaign_name}",
                    key=f"campaign_{campaign_name}",
                    use_container_width=True,
                    type="secondary"
                ):
                    st.session_state.selected_campaign = campaign_name
                    st.rerun()
        else:
            st.info("No campaigns found. Create a campaign by saving influencers from the Discover page.")

# Discover Page Function
def render_discover_page():
    """Render the Discover page (homepage) with all search logic."""
    # Title and header
    st.title("🎯 TikTok Influencer Search")
    st.markdown("Discover influencers by hashtag and follower count")
    
    # Filter button at the top
    if st.button('⚙️ Edit Search Filters'):
        filter_dialog()
    
    # Get search filters from session state
    hashtag = st.session_state.get('hashtag', '')
    results_limit = st.session_state.get('results_limit', 10)
    location_filter = st.session_state.get('location_filter', 'USA')
    min_followers = st.session_state.get('min_followers', 0)
    max_followers = st.session_state.get('max_followers', 999999999)
    
    # Check if search was triggered (from Apply Filters button)
    search_button = st.session_state.get('search_button_clicked', False)
    
    # Main content area
    if search_button:
        if not hashtag.strip():
            st.warning("⚠️ Please enter a hashtag to search")
        else:
            # Split comma-separated hashtags into a list
            hashtag_list = [tag.strip() for tag in hashtag.split(',') if tag.strip()]
            
            if not hashtag_list:
                st.warning("⚠️ Please enter at least one valid hashtag")
            else:
                hashtags_display = ', '.join([f'#{tag}' for tag in hashtag_list])
                hashtag_key = ','.join(sorted(hashtag_list))  # Normalize hashtag list for comparison
                
                # Check if hashtag has changed
                last_searched_hashtag = st.session_state.get('last_searched_hashtag', '')
                hashtag_changed = hashtag_key != last_searched_hashtag
                
                # Check if results_limit has increased (for same hashtag)
                last_searched_results_limit = st.session_state.get('last_searched_results_limit', 0)
                results_limit_increased = not hashtag_changed and results_limit > last_searched_results_limit
                
                if hashtag_changed or results_limit_increased:
                    # New hashtag or increased results limit - run Apify scraper
                    with st.spinner(f'🔍 Searching for {hashtags_display}...'):
                        try:
                            # Scrape raw results (without follower filtering)
                            raw_influencers, total_videos = scrape_tiktok_influencers_raw(
                                hashtag_list,
                                APIFY_API_TOKEN,
                                results_limit
                            )
                            
                            if total_videos == 0:
                                st.info(f"📭 No results found for hashtag(s): {hashtags_display}")
                            else:
                                # Store raw results in session state
                                st.session_state['raw_results'] = raw_influencers
                                st.session_state['raw_results_count'] = total_videos
                                # Update last searched hashtag and results limit only after successful scrape
                                st.session_state['last_searched_hashtag'] = hashtag_key
                                st.session_state['last_searched_results_limit'] = results_limit
                                
                        except Exception as e:
                            st.error(f"❌ Error occurred: {str(e)}")
                            st.info("💡 Make sure your APIFY_API_TOKEN is valid and you're using the correct actor ID")
                            raw_influencers = []
                            total_videos = 0
                else:
                    # Same hashtag and same or lower results limit - use existing raw results
                    raw_influencers = st.session_state.get('raw_results', [])
                    total_videos = st.session_state.get('raw_results_count', 0)
                    st.info(f"🔄 Using cached results for {hashtags_display}. Adjusting filters...")
                
                # Filter by follower range and location (always filter, whether using new or cached data)
                if raw_influencers:
                    # First filter by followers
                    filtered_by_followers = filter_by_followers(
                        raw_influencers,
                        min_followers,
                        max_followers
                    )
                    
                    # Then filter by location
                    filtered_influencers = filter_by_location(
                        filtered_by_followers,
                        location_filter
                    )
                    
                    # Store filtered results in session state for homepage display
                    st.session_state['past_search_results'] = filtered_influencers
                    st.session_state['past_search_count'] = total_videos
                    st.session_state['past_search_hashtags'] = hashtags_display
                    
                    # Display results
                    if hashtag_changed or results_limit_increased:
                        st.success(f"✅ Found {len(filtered_influencers)} unique influencers (from {total_videos} total videos)")
                    else:
                        st.success(f"✅ Found {len(filtered_influencers)} unique influencers (from {total_videos} total videos) with current filters")
                    
                    if filtered_influencers:
                        st.subheader("📊 Influencer Results")
                        
                        # Display influencers vertically (one per row)
                        for idx, influencer in enumerate(filtered_influencers):
                            display_influencer_card(influencer, card_key=f"search_{idx}")
                            st.markdown("<br>", unsafe_allow_html=True)
                    else:
                        st.info(f"📭 No influencers found with {min_followers:,} - {max_followers:,} followers")

    else:
        # Display past search results on homepage
        if 'past_search_results' in st.session_state and st.session_state['past_search_results']:
            past_results = st.session_state['past_search_results']
            past_count = st.session_state.get('past_search_count', 0)
            past_hashtags = st.session_state.get('past_search_hashtags', 'previous search')
            
            st.success(f"✅ Previous search: Found {len(past_results)} unique influencers (from {past_count} total videos) for {past_hashtags}")
            st.subheader("📊 Past Search Results")
            
            # Display influencers vertically (one per row)
            for idx, influencer in enumerate(past_results):
                display_influencer_card(influencer, card_key=f"past_{idx}")
                st.markdown("<br>", unsafe_allow_html=True)
        else:

            
            # Example cards layout demonstration
            st.markdown("### Example Layout")
            st.markdown("Results will be displayed as cards showing:")
            st.markdown("- Profile image")
            st.markdown("- Username and verification status")
            st.markdown("- Follower count")
            st.markdown("- Bio")
            st.markdown("- Link to TikTok profile")

# Sidebar Navigation
with st.sidebar:
    # Navigation menu
    menu_options = ['Discover', 'Campaign', 'Templates']
    menu_icons = ['search', 'megaphone', 'envelope']
    
    # Determine default index
    current_page = st.session_state.current_page
    if current_page == 'Discover':
        default_idx = 0
    elif current_page == 'Campaign':
        default_idx = 1
    elif current_page == 'Templates':
        default_idx = 2
    else:
        default_idx = 0
    
    selected = option_menu(
        menu_title=None,
        options=menu_options,
        icons=menu_icons, 
        menu_icon="cast",
        default_index=default_idx,
        styles={
            "container": {"padding": "0!important", "background-color": "transparent", "border-radius": "0", "width": "100%", "margin": "0"},
            "icon": { "color": "inherit","font-size": "15px"}, 
            "nav-link": {"font-size": "14px", "text-align": "left", "color": "inherit", "background-color": "transparent"},
            "nav-link-selected": {"background-color": "rgba(128, 128, 128, 0.1)", "color": "inherit", "border-radius": "8px"},

        }
    )
    
    # Update page state when selection changes
    if selected != st.session_state.current_page:
        st.session_state.current_page = selected
        # Smart triggering: sync campaigns if user clicks 'Campaign' and list is None
        if selected == 'Campaign' and st.session_state.campaign_list is None:
            sync_campaigns()
        st.rerun()
    
    # Sync button at bottom of sidebar
    st.divider()
    if st.button("🔄 Sync with Google Sheets", use_container_width=True):
        st.toast("Syncing data...")
        sync_campaigns(force=True)
        st.rerun()

def deduplicate_influencers(influencers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate influencers by authorMeta.name or id.
    Priority: id > name
    """
    seen_ids = set()
    seen_names = set()
    unique_influencers = []
    
    for influencer in influencers:
        author_meta = influencer.get('authorMeta', {})
        influencer_id = author_meta.get('id')
        influencer_name = author_meta.get('name')
        
        # Check if we've seen this influencer before
        if influencer_id and influencer_id in seen_ids:
            continue
        if influencer_name and influencer_name in seen_names:
            continue
        
        # Add to seen sets
        if influencer_id:
            seen_ids.add(influencer_id)
        if influencer_name:
            seen_names.add(influencer_name)
        
        unique_influencers.append(influencer)
    
    return unique_influencers

def filter_by_followers(influencers: List[Dict[str, Any]], min_followers: int, max_followers: int) -> List[Dict[str, Any]]:
    """Filter influencers by follower count range."""
    filtered = []
    NO_MIN = 0
    NO_MAX = 999999999
    
    for influencer in influencers:
        author_meta = influencer.get('authorMeta', {})
        followers = author_meta.get('fans', 0)
        
        # Handle "no min" and "no max" cases
        min_check = True if min_followers == NO_MIN else followers >= min_followers
        max_check = True if max_followers == NO_MAX else followers <= max_followers
        
        if min_check and max_check:
            filtered.append(influencer)
    
    return filtered

def is_us_influencer(influencer: Dict[str, Any]) -> bool:
    """
    Check if an influencer is from US based on subtitle links and bio.
    Returns True if:
    1. First subtitle (index 0) has language code '48' or 'eng-US', OR
    2. '48' appears anywhere in subtitle links, OR
    3. Bio contains 'USA'
    """
    recent_videos = influencer.get('recent_videos', [])
    author_meta = influencer.get('authorMeta', {})
    bio = author_meta.get('signature', '').upper()
    
    # Check bio for 'USA' (backup method)
    if 'USA' in bio:
        return True
    
    # Check videos for subtitle links
    for video in recent_videos:
        video_meta = video.get('videoMeta', {})
        
        # Check if subtitleLinks exists
        if 'subtitleLinks' not in video_meta:
            continue
            
        subtitle_links = video_meta.get('subtitleLinks', [])
        
        if not subtitle_links or len(subtitle_links) == 0:
            continue
        
        # Primary check: First subtitle (index 0) has language code '48' or 'eng-US'
        first_subtitle = subtitle_links[0]
        if isinstance(first_subtitle, dict):
            lang_code = str(first_subtitle.get('languageCode', '') or first_subtitle.get('language', '') or '')
            if lang_code == '48' or lang_code == 'eng-US':
                return True
        elif isinstance(first_subtitle, str):
            if first_subtitle == '48' or first_subtitle == 'eng-US':
                return True
        
        # Backup check: '48' appears anywhere in the subtitle links list
        for subtitle in subtitle_links:
            if isinstance(subtitle, dict):
                lang_code = str(subtitle.get('languageCode', '') or subtitle.get('language', '') or '')
                if lang_code == '48':
                    return True
            elif isinstance(subtitle, str):
                if '48' in str(subtitle):
                    return True
    
    return False

def filter_by_location(influencers: List[Dict[str, Any]], location: str) -> List[Dict[str, Any]]:
    """Filter influencers by location."""
    if location == "All":
        return influencers
    elif location == "US":
        return [inf for inf in influencers if is_us_influencer(inf)]
    else:
        return influencers

def group_videos_by_author(videos: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group videos by author and collect their recent content.
    Returns a dictionary mapping author ID to list of their videos.
    """
    authors_dict = {}
    
    for video in videos:
        author_meta = video.get('authorMeta', {})
        author_id = author_meta.get('id') or author_meta.get('name', 'unknown')
        
        if author_id not in authors_dict:
            authors_dict[author_id] = []
        
        authors_dict[author_id].append(video)
    
    # Sort videos by createTime (most recent first) and keep only 1 most recent
    for author_id in authors_dict:
        videos_list = authors_dict[author_id]
        # Sort by createTime if available, otherwise keep original order
        try:
            videos_list.sort(key=lambda x: x.get('createTime', 0), reverse=True)
        except:
            pass
        authors_dict[author_id] = videos_list[:1]  # Keep only 1 most recent
    
    return authors_dict

def format_number(num: int) -> str:
    """Format large numbers to K/M format."""
    if num >= 1000000:
        return f"{num / 1000000:.1f}M"
    elif num >= 1000:
        return f"{num / 1000:.1f}K"
    else:
        return str(num)

@st.cache_data
def scrape_tiktok_influencers_raw(hashtag_list: List[str], apify_token: str, results_limit: int = 10) -> tuple:
    """
    Scrape TikTok influencers using Apify and return raw results (before filtering by followers).
    Returns: (raw_influencers_list, total_videos_count)
    """
    # Create Apify client from token
    apify_client = ApifyClient(apify_token)
    
    # Prepare input for Apify TikTok Data Extractor
    actor_input = {
        'hashtags': hashtag_list,
        'resultsLimit': results_limit,
        'resultsPerPage': results_limit,
        'searchSection': '/video',
    }
    
    # Run the Apify actor and wait for completion
    run = apify_client.actor('clockworks/free-tiktok-scraper').call(run_input=actor_input)
    
    # Fetch results from the default dataset
    dataset_items = apify_client.dataset(run['defaultDatasetId']).list_items().items
    
    if not dataset_items:
        return [], 0
    
    # Process results - group videos by author
    videos = list(dataset_items)
    authors_dict = group_videos_by_author(videos)
    
    # Create influencer objects with authorMeta and recent_videos
    influencers_list = []
    for author_id, author_videos in authors_dict.items():
        if author_videos:
            # Use the first video to get authorMeta (all videos from same author have same authorMeta)
            first_video = author_videos[0]
            influencer_data = {
                'authorMeta': first_video.get('authorMeta', {}),
                'recent_videos': author_videos
            }
            influencers_list.append(influencer_data)
    
    return influencers_list, len(videos)

def display_influencer_card(influencer_data: Dict[str, Any], card_key: str = None):
    """
    Display a single influencer card with profile info and 1 recent video.
    influencer_data should contain 'authorMeta' and 'recent_videos' (list with 1 video).
    card_key: Unique key for the button (defaults to influencer name)
    """
    if card_key is None:
        author_meta = influencer_data.get('authorMeta', {})
        card_key = author_meta.get('name', 'unknown') + '_' + str(author_meta.get('id', ''))
    influencer_data['_card_key'] = card_key
    author_meta = influencer_data.get('authorMeta', {})
    recent_videos = influencer_data.get('recent_videos', [])
    
    name = author_meta.get('name', 'Unknown')
    avatar_url = author_meta.get('avatar', '')
    followers = author_meta.get('fans', 0)
    signature = author_meta.get('signature', 'No bio available')
    verified = author_meta.get('verified', False)
    handle = author_meta.get('authorMeta.name') or author_meta.get('name', 'unknown')
    
    # Create profile URL
    profile_url = f"https://www.tiktok.com/@{handle}" if handle != 'unknown' else '#'
    
    # Format follower count
    followers_str = format_number(followers)
    
    # Card container with border styling
    st.markdown(
        """
        <div style="
            border: 0.5px solid #ffffff;
            border-radius: 12px;
            margin-bottom: 30px;
        ">
        """,
        unsafe_allow_html=True
    )
    
    # Create 5 columns: 1. avatar, 2. name/handle/signature, 3. followers, 4. video content, 5. add to campaign
    col1, col2, col3, col4, col5 = st.columns([1, 2, 1, 1.5, 1])
    
    # Column 1: Author avatar
    with col1:
        st.markdown('<div style="display: flex; align-items: center; height: 100%;">', unsafe_allow_html=True)
        if avatar_url:
            try:
                st.markdown(
                    f'<img src="{avatar_url}" style="width: 80px; height: 80px; border-radius: 50%; object-fit: cover;" />',
                    unsafe_allow_html=True
                )
            except:
                st.markdown(
                    '<img src="https://via.placeholder.com/80" style="width: 80px; height: 80px; border-radius: 50%; object-fit: cover;" />',
                    unsafe_allow_html=True
                )
        else:
            st.markdown(
                '<img src="https://via.placeholder.com/80" style="width: 80px; height: 80px; border-radius: 50%; object-fit: cover;" />',
                unsafe_allow_html=True
            )
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Column 2: Name, handle, signature
    with col2:
        st.markdown('<div style="display: flex; flex-direction: column; justify-content: center; height: 100%;">', unsafe_allow_html=True)
        # Name with verified badge as hyperlink
        verified_badge = " ✓" if verified else ""
        if profile_url and profile_url != '#':
            st.markdown(
                f'<h3><a href="{profile_url}" target="_blank" style="text-decoration: none; color: inherit;">{name}{verified_badge}</a></h3>',
                unsafe_allow_html=True
            )
        else:
            st.markdown(f"### {name}{verified_badge}")
        
        # Handle
        st.markdown(f"@{handle}")
        
        # Bio/Signature
        if signature and signature != 'No bio available':
            st.markdown(f"_{signature}_")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Column 3: Followers
    with col3:
        st.markdown('<div style="display: flex; align-items: center; height: 100%;">', unsafe_allow_html=True)
        st.markdown(f"**{followers_str} Followers**")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Column 4: Recent video content
    with col4:
        st.markdown('<div style="display: flex; flex-direction: column; justify-content: center; height: 100%;">', unsafe_allow_html=True)
        # Recent video section
        if recent_videos and len(recent_videos) > 0:
            # Display the single video
            video = recent_videos[0]
            
            # Get cover URL from videoMeta.coverUrl
            video_meta = video.get('videoMeta', {})
            cover_url = video_meta.get('coverUrl', '') or video.get('coverUrl', '')
            
            # Get stats (try top level first, then videoMeta)
            digg_count = video.get('diggCount', 0) or video_meta.get('diggCount', 0)
            play_count = video.get('playCount', 0) or video_meta.get('playCount', 0)
            comment_count = video.get('commentCount', 0) or video_meta.get('commentCount', 0)
            web_video_url = video.get('webVideoUrl', '#') or video_meta.get('webVideoUrl', '#')
            
            # Video thumbnail as clickable link with 1:1 aspect ratio (160px = 2x of 80px avatar) and play icon overlay
            play_icon_svg = '''
            <svg width="40" height="40" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); opacity: 0.9;">
                <path d="M8 5v14l11-7z" fill="white"/>
            </svg>
            '''
            
            # Build video thumbnail HTML
            video_thumbnail_html = ''
            if cover_url:
                try:
                    # Create clickable image link with fixed 160px width, 1:1 aspect ratio, and play icon overlay
                    if web_video_url and web_video_url != '#':
                        video_thumbnail_html = (
                            f'<a href="{web_video_url}" target="_blank" style="position: relative; display: inline-block;">'
                            f'<img src="{cover_url}" style="width: 160px; height: 160px; object-fit: cover; border-radius: 8px; cursor: pointer; display: block;" />'
                            f'{play_icon_svg}'
                            f'</a>'
                        )
                    else:
                        video_thumbnail_html = (
                            f'<div style="position: relative; display: inline-block;">'
                            f'<img src="{cover_url}" style="width: 160px; height: 160px; object-fit: cover; border-radius: 8px; display: block;" />'
                            f'{play_icon_svg}'
                            f'</div>'
                        )
                except:
                    placeholder_html = f'<div style="position: relative; display: inline-block;"><img src="https://via.placeholder.com/160" style="width: 160px; height: 160px; object-fit: cover; border-radius: 8px; display: block;" />{play_icon_svg}</div>'
                    if web_video_url and web_video_url != '#':
                        video_thumbnail_html = f'<a href="{web_video_url}" target="_blank">{placeholder_html}</a>'
                    else:
                        video_thumbnail_html = placeholder_html
            else:
                placeholder_html = f'<div style="position: relative; display: inline-block;"><img src="https://via.placeholder.com/160" style="width: 160px; height: 160px; object-fit: cover; border-radius: 8px; display: block;" />{play_icon_svg}</div>'
                if web_video_url and web_video_url != '#':
                    video_thumbnail_html = f'<a href="{web_video_url}" target="_blank">{placeholder_html}</a>'
                else:
                    video_thumbnail_html = placeholder_html
            
            # Build stats HTML
            stats_html = (
                f'<div style="margin-top: 8px; color: #333;">'
                f'▶️ {format_number(play_count)} &nbsp; '
                f'👍 {format_number(digg_count)} &nbsp; '
                f'💬 {format_number(comment_count)}'
                f'</div>'
            )
            
            # Combine everything in a single wrapper div with white background
            content_html = (
                '<div style="background-color: rgba(255, 255, 255, 0.7); padding: 12px; border-radius: 8px; display: inline-flex; flex-direction: column; align-items: center; width: fit-content;">'
                '<div style="font-size: 12px; color: #333; margin-bottom: 8px; font-weight: 500;">#Hashtag video</div>'
                f'{video_thumbnail_html}'
                f'{stats_html}'
                '</div>'
            )
            
            st.markdown(content_html, unsafe_allow_html=True)
        else:
            st.markdown("_No recent content available_")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Column 5: Save to Campaign popover
    with col5:
        st.markdown('<div style="display: flex; align-items: center; height: 100%;">', unsafe_allow_html=True)
        
        card_key = influencer_data.get('_card_key', 'default')
        author_meta = influencer_data.get('authorMeta', {})
        handle = author_meta.get('nickName') or author_meta.get('name', 'unknown')
        unique_key = f"{handle}_{author_meta.get('id', card_key)}"

        popover_label_key = f"campaign_label_{unique_key}"
        selected_campaign_key = f"selected_campaign_{unique_key}"
        success_message_key = f"success_message_{unique_key}"
        popover_label = st.session_state.get(popover_label_key, "Add to Campaign")
        
        # Display success message if it exists in session state
        if success_message_key in st.session_state:
            st.success(st.session_state[success_message_key])
            del st.session_state[success_message_key]  # Clear after displaying
        
        # Popover with campaign selection
        with st.popover(popover_label, use_container_width=True):
            # Smart triggering: sync campaigns if list is None
            if st.session_state.campaign_list is None:
                sync_campaigns()
            
            # Use session state campaign list
            campaign_names = st.session_state.campaign_list if st.session_state.campaign_list is not None else []

            # Create campaign section
            
            create_form_open_key = f"create_campaign_open_{unique_key}"
            show_create_form = st.session_state.get(create_form_open_key, False)

            if st.button("➕ Create campaign", key=f"create_campaign_btn_{unique_key}", use_container_width=True):
                st.session_state[create_form_open_key] = not show_create_form
                show_create_form = not show_create_form

            if show_create_form:
                new_campaign_name = st.text_input(
                    "Campaign name",
                    key=f"new_campaign_input_{unique_key}",
                    placeholder="Enter campaign name"
                )
                if st.button(
                    "Save new campaign",
                    key=f"save_new_campaign_btn_{unique_key}",
                    use_container_width=True,
                    type="primary"
                ):
                
                    clean_name = new_campaign_name.strip()
                    if not clean_name:
                        st.warning("Please enter a campaign name.")
                    else:
                        if create_campaign(clean_name):
                            st.toast(f"✅ Campaign '{clean_name}' created")
                            st.session_state[selected_campaign_key] = clean_name
                            st.session_state[create_form_open_key] = False
                            # Update campaign_names from session state (already updated in create_campaign)
                            campaign_names = st.session_state.campaign_list if st.session_state.campaign_list is not None else []
                        else:
                            st.error("❌ Failed to create campaign")
                            

           
            selected_campaign = st.session_state.get(selected_campaign_key)
        
            if campaign_names:
                for campaign in campaign_names:
                    if st.button(
                        campaign,
                        key=f"campaign_btn_{unique_key}_{campaign}",
                        use_container_width=True
                    ):
                        st.session_state[selected_campaign_key] = campaign
                        selected_campaign = campaign
                        if save_to_gsheet(influencer_data, campaign):
                            st.session_state[popover_label_key] = campaign
                            st.session_state[success_message_key] = f"✅ Added to {campaign}"
                            st.rerun()
                        else:
                            st.error("❌ Failed to save")
            else:
                st.info("No campaigns found. Please create a campaign in your Google Sheet.")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("</div>", unsafe_allow_html=True)

# Main content router
if st.session_state.current_page == 'Campaign':
    render_campaign_page()
elif st.session_state.current_page == 'Templates':
    render_templates_page()
else:
    render_discover_page()
