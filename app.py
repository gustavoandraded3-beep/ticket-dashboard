import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

# Configuration for Manage Engine
MANAGE_ENGINE_COLUMNS = [
    'Request ID',
    'Subject',
    'Status.Name',
    'Group.Name',
    'Sub Category.Name',
    'IPC Feature List',
    'Technician.Name',
    'Requester.Name',
    'Created Date',
    'Completed Time',
    'Last Updated Time',
    'DevOpsRef',
    'Category.Name',
    'Priority.Name',
    'IPC Feature',
    'Responded Time'
]

# Configuration for ConnectWise
CONNECTWISE_COLUMNS = [
    'Ticket #',
    'Summary Description',
    'Contact',
    'Ticket Owner',
    'Status',
    'Entered',
    'Priority',
    'Entered By',
    'Type',
    'Subtype',
    'Item',
    '3rd Party Ref',
    'Closed On'
]

# MANDATORY: Normalized closed statuses (lowercase)
CLOSED_STATUSES = {'closed', 'resolved', 'completed'}


def validate_csv(df, system_type):
    """
    Validate that all required columns are present in the dataframe based on system type.
    Returns (is_valid, missing_columns)
    """
    if system_type == "Manage Engine":
        required = MANAGE_ENGINE_COLUMNS
    else:  # ConnectWise
        required = CONNECTWISE_COLUMNS
    
    missing = [col for col in required if col not in df.columns]
    return len(missing) == 0, missing


def extract_priority_from_image_path(value):
    """
    Extract priority name from image path or return the value as-is.
    Handles ConnectWise priority format: path/color.gif (count)
    """
    import re
    
    if pd.isna(value):
        return 'Unassigned'
    
    value_str = str(value).strip()
    
    # Try to extract color name from image path
    # Pattern: something/color.gif or color.gif
    match = re.search(r'([a-zA-Z]+)\.gif', value_str)
    if match:
        color = match.group(1).capitalize()
        return color
    
    # If no image path found, return original value
    return value_str if value_str else 'Unassigned'


def normalize_connectwise_to_manage_engine(df):
    """
    Convert ConnectWise dataframe to Manage Engine schema.
    Maps ConnectWise columns to Manage Engine columns.
    """
    df_normalized = pd.DataFrame()
    
    # Map ConnectWise columns to Manage Engine columns
    df_normalized['Request ID'] = df['Ticket #']
    df_normalized['Subject'] = df['Summary Description']
    df_normalized['Status.Name'] = df['Status']
    df_normalized['Group.Name'] = df['Type']
    df_normalized['Sub Category.Name'] = df['Subtype']
    df_normalized['IPC Feature List'] = df['Item']
    df_normalized['Technician.Name'] = df['Ticket Owner']
    df_normalized['Requester.Name'] = df['Contact']
    df_normalized['Created Date'] = df['Entered']
    df_normalized['Completed Time'] = df['Closed On']
    df_normalized['Last Updated Time'] = df['Closed On']
    df_normalized['DevOpsRef'] = df['3rd Party Ref']
    # Extract priority from image path format
    df_normalized['Priority.Name'] = df['Priority'].apply(extract_priority_from_image_path)
    df_normalized['Category.Name'] = df['Type']
    df_normalized['IPC Feature'] = df['Item']
    df_normalized['Responded Time'] = df['Entered']
    
    return df_normalized


def parse_date_column(series):
    """
    Parse a date column, handling various formats and returning date only (no time).
    Returns a Series of datetime.date objects or NaT.
    """
    parsed = pd.to_datetime(series, errors='coerce', dayfirst=False)
    return parsed.dt.date


def format_date_display(date_obj):
    """
    Format a date object to dd/mm/yyyy string.
    """
    if pd.isna(date_obj):
        return ''
    if isinstance(date_obj, str):
        return date_obj
    try:
        return date_obj.strftime('%d/%m/%Y')
    except:
        return str(date_obj)


def replace_blank_with_unassigned(series):
    """
    Replace NaN, empty strings, and 'nan' with 'Unassigned'.
    """
    result = series.copy()
    result = result.fillna('Unassigned')
    result = result.replace('', 'Unassigned')
    result = result.replace('nan', 'Unassigned')
    result = result.astype(str)
    result = result.replace('nan', 'Unassigned')
    return result


def prepare_dataframe(df):
    """
    Prepare the dataframe by:
    - Normalizing status
    - Parsing date columns
    - Determining closed status
    - Calculating the effective closed date
    - Replacing blanks with 'Unassigned' for breakdown columns
    """
    df = df.copy()
    
    # Strip whitespace from string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    
    # MANDATORY: Normalize status to lowercase for comparison
    df['Status Clean'] = df['Status.Name'].astype(str).str.strip().str.lower()
    
    # Parse date columns (convert to date only, no time)
    df['Created Date Parsed'] = parse_date_column(df['Created Date'])
    df['Completed Time Parsed'] = parse_date_column(df['Completed Time'])
    df['Last Updated Time Parsed'] = parse_date_column(df['Last Updated Time'])
    
    # Determine if ticket is closed
    df['Is Closed'] = df['Status Clean'].isin(CLOSED_STATUSES)
    
    # Calculate effective closed date (ClosedDT logic)
    df['ClosedDT'] = df.apply(
        lambda row: row['Completed Time Parsed'] if pd.notna(row['Completed Time Parsed']) 
        else row['Last Updated Time Parsed'] if row['Is Closed'] else pd.NaT,
        axis=1
    )
    
    # Replace blanks with 'Unassigned' for breakdown columns
    df['Group.Name'] = replace_blank_with_unassigned(df['Group.Name'])
    df['Sub Category.Name'] = replace_blank_with_unassigned(df['Sub Category.Name'])
    df['IPC Feature List'] = replace_blank_with_unassigned(df['IPC Feature List'])
    df['Technician.Name'] = replace_blank_with_unassigned(df['Technician.Name'])
    df['Requester.Name'] = replace_blank_with_unassigned(df['Requester.Name'])
    df['DevOpsRef'] = replace_blank_with_unassigned(df['DevOpsRef'])
    df['Priority.Name'] = replace_blank_with_unassigned(df['Priority.Name'])
    
    return df


def get_open_tickets(df):
    """Get all open tickets (status is NOT in closed set)."""
    return df[~df['Is Closed']].copy()


def get_tickets_opened_on_date(df, target_date):
    """Get tickets that were opened on a specific date."""
    return df[df['Created Date Parsed'] == target_date].copy()


def get_tickets_closed_on_date(df, target_date):
    """Get tickets that were closed on a specific date."""
    return df[(df['Is Closed']) & (df['ClosedDT'] == target_date)].copy()


def get_tickets_opened_since(df, cutoff_datetime):
    """Get tickets opened since cutoff."""
    cutoff_date = cutoff_datetime.date()
    return df[df['Created Date Parsed'] >= cutoff_date].copy()


def get_tickets_closed_since(df, cutoff_datetime):
    """Get tickets closed since cutoff."""
    cutoff_date = cutoff_datetime.date()
    return df[(df['Is Closed']) & (df['ClosedDT'] >= cutoff_date)].copy()


def get_tickets_in_period(df, date_a, date_b, scope_type):
    """Get tickets based on scope type and period (Date A to Date B inclusive)."""
    if scope_type == 'open':
        return get_open_tickets(df)
    elif scope_type == 'all':
        return df.copy()
    elif scope_type == 'created_in_period':
        return df[
            (df['Created Date Parsed'] >= date_a) & 
            (df['Created Date Parsed'] <= date_b)
        ].copy()
    elif scope_type == 'closed_in_period':
        return df[
            (df['Is Closed']) & 
            (df['ClosedDT'] >= date_a) & 
            (df['ClosedDT'] <= date_b)
        ].copy()
    else:
        return df.copy()


def count_by_column(df, column_name):
    """Count tickets grouped by a specific column."""
    if len(df) == 0:
        return pd.DataFrame(columns=[column_name, 'Count'])
    
    counts = df[column_name].value_counts().reset_index()
    counts.columns = [column_name, 'Count']
    return counts.sort_values('Count', ascending=False)


def get_current_year_metrics(df):
    """Calculate metrics for the current year."""
    current_year = datetime.now().year
    
    created_this_year = df[df['Created Date Parsed'].apply(
        lambda x: x.year == current_year if pd.notna(x) else False
    )].copy()
    
    created_year_open = created_this_year[~created_this_year['Is Closed']]
    created_year_closed = created_this_year[created_this_year['Is Closed']]
    
    closed_this_year = df[
        (df['Is Closed']) & 
        (df['ClosedDT'].apply(lambda x: x.year == current_year if pd.notna(x) else False))
    ]
    
    return {
        'year': current_year,
        'created_total': len(created_this_year),
        'created_open': len(created_year_open),
        'created_closed': len(created_year_closed),
        'closed_total': len(closed_this_year)
    }


def get_daily_trend_data(df, days=30):
    """Get daily opened vs closed counts for the last N days."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days-1)
    
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    trend_data = []
    for date in date_range:
        date_only = date.date()
        opened = len(df[df['Created Date Parsed'] == date_only])
        closed = len(df[(df['Is Closed']) & (df['ClosedDT'] == date_only)])
        
        trend_data.append({
            'Date': date_only,
            'Opened': opened,
            'Closed': closed
        })
    
    return pd.DataFrame(trend_data)


def generate_email_summary(df, date_a, date_b):
    """Generate an email-ready summary of ticket metrics."""
    now = datetime.now()
    
    open_tickets = get_open_tickets(df)
    total_open = len(open_tickets)
    
    opened_a = len(get_tickets_opened_on_date(df, date_a))
    closed_a = len(get_tickets_closed_on_date(df, date_a))
    opened_b = len(get_tickets_opened_on_date(df, date_b))
    closed_b = len(get_tickets_closed_on_date(df, date_b))
    
    last_24h_opened = len(get_tickets_opened_since(df, now - timedelta(hours=24)))
    last_24h_closed = len(get_tickets_closed_since(df, now - timedelta(hours=24)))
    
    last_7d_opened = len(get_tickets_opened_since(df, now - timedelta(days=7)))
    last_7d_closed = len(get_tickets_closed_since(df, now - timedelta(days=7)))
    
    last_30d_opened = len(get_tickets_opened_since(df, now - timedelta(days=30)))
    last_30d_closed = len(get_tickets_closed_since(df, now - timedelta(days=30)))
    
    by_group = count_by_column(open_tickets, 'Group.Name')
    by_subcategory = count_by_column(open_tickets, 'Sub Category.Name')
    by_ipc = count_by_column(open_tickets, 'IPC Feature List')
    by_technician = count_by_column(open_tickets, 'Technician.Name')
    
    date_a_str = format_date_display(date_a)
    date_b_str = format_date_display(date_b)
    now_str = now.strftime('%d/%m/%Y at %I:%M %p')
    
    summary = f"""TICKET SYSTEM SUMMARY
Generated on: {now_str}

OVERVIEW
• Total Open Tickets: {total_open}

DATE COMPARISON

Specific Dates:
Date A ({date_a_str}):
• Tickets Opened: {opened_a}
• Tickets Closed/Resolved: {closed_a}

Date B ({date_b_str}):
• Tickets Opened: {opened_b}
• Tickets Closed/Resolved: {closed_b}

Rolling Periods:
Last 24 Hours:
• Tickets Opened: {last_24h_opened}
• Tickets Closed/Resolved: {last_24h_closed}

Last 7 Days:
• Tickets Opened: {last_7d_opened}
• Tickets Closed/Resolved: {last_7d_closed}

Last Month (30 days):
• Tickets Opened: {last_30d_opened}
• Tickets Closed/Resolved: {last_30d_closed}

OPEN TICKETS BREAKDOWN

By Group:
"""
    
    if len(by_group) > 0:
        for _, row in by_group.iterrows():
            summary += f"• {row['Group.Name']}: {row['Count']} tickets\n"
    else:
        summary += "• No open tickets\n"
    
    summary += "\nBy Sub-Category:\n"
    if len(by_subcategory) > 0:
        for _, row in by_subcategory.head(10).iterrows():
            summary += f"• {row['Sub Category.Name']}: {row['Count']} tickets\n"
        if len(by_subcategory) > 10:
            summary += f"• ... and {len(by_subcategory) - 10} more categories\n"
    else:
        summary += "• No open tickets\n"
    
    summary += "\nBy IPC Feature:\n"
    if len(by_ipc) > 0:
        for _, row in by_ipc.head(10).iterrows():
            summary += f"• {row['IPC Feature List']}: {row['Count']} tickets\n"
        if len(by_ipc) > 10:
            summary += f"• ... and {len(by_ipc) - 10} more features\n"
    else:
        summary += "• No open tickets\n"
    
    summary += "\nBy Technician:\n"
    if len(by_technician) > 0:
        for _, row in by_technician.iterrows():
            summary += f"• {row['Technician.Name']}: {row['Count']} tickets\n"
    else:
        summary += "• No open tickets\n"
    
    summary += "\n---\nThis summary was generated automatically from the ticket system export.\n"
    
    return summary


def display_breakdown_with_drilldown(tickets_df, column_name, label):
    """Display a breakdown with expandable drill-down for each group."""
    counts = count_by_column(tickets_df, column_name)
    
    if len(counts) == 0:
        st.info(f"No tickets in selected scope")
        return
    
    st.write(f"**Total groups: {len(counts)}**")
    
    for _, row in counts.iterrows():
        group_name = row[column_name]
        count = row['Count']
        
        with st.expander(f"➕ {group_name} ({count})"):
            group_tickets = tickets_df[tickets_df[column_name] == group_name]
            
            display_df = group_tickets[[
                'Request ID', 'Subject', 'Status.Name', 'Group.Name', 
                'Requester.Name', 'Technician.Name', 'Created Date Parsed', 'Completed Time Parsed'
            ]].copy()
            
            display_df['Created Date'] = display_df['Created Date Parsed'].apply(format_date_display)
            display_df['Completed Time'] = display_df['Completed Time Parsed'].apply(format_date_display)
            
            # Rename columns for display
            display_df = display_df.rename(columns={
                'Status.Name': 'Status',
                'Group.Name': 'Type',
                'Requester.Name': 'Requester',
                'Technician.Name': 'Technician'
            })
            
            display_df = display_df[[
                'Request ID', 'Subject', 'Status', 'Type', 
                'Requester', 'Technician', 'Created Date', 'Completed Time'
            ]]
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)


def display_devops_breakdown(tickets_df):
    """Display DevOps breakdown with drill-down."""
    only_real_devops = st.checkbox("Only show tickets with DevOpsRef", value=True)
    
    if only_real_devops:
        filtered_tickets = tickets_df[tickets_df['DevOpsRef'] != 'Unassigned']
    else:
        filtered_tickets = tickets_df
    
    counts = count_by_column(filtered_tickets, 'DevOpsRef')
    
    if len(counts) == 0:
        st.info("No tickets on DevOps in selected scope")
        return
    
    st.write(f"**Total DevOps references: {len(counts)}**")
    
    for _, row in counts.iterrows():
        devops_ref = row['DevOpsRef']
        count = row['Count']
        
        with st.expander(f"➕ {devops_ref} ({count})"):
            group_tickets = filtered_tickets[filtered_tickets['DevOpsRef'] == devops_ref]
            
            display_df = group_tickets[[
                'DevOpsRef', 'Request ID', 'Subject', 'Status.Name', 'Group.Name', 
                'Requester.Name', 'Technician.Name', 'Created Date Parsed', 'Completed Time Parsed'
            ]].copy()
            
            display_df['Created Date'] = display_df['Created Date Parsed'].apply(format_date_display)
            display_df['Completed Time'] = display_df['Completed Time Parsed'].apply(format_date_display)
            
            # Rename columns for display
            display_df = display_df.rename(columns={
                'Status.Name': 'Status',
                'Group.Name': 'Type',
                'Requester.Name': 'Requester',
                'Technician.Name': 'Technician'
            })
            
            display_df = display_df[[
                'DevOpsRef', 'Request ID', 'Subject', 'Status', 'Type', 
                'Requester', 'Technician', 'Created Date', 'Completed Time'
            ]]
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)


def display_abandoned_tickets(tickets_df):
    """Display abandoned tickets breakdown based on days since last update."""
    now = datetime.now().date()
    
    abandoned_data = tickets_df.copy()
    abandoned_data['Days Since Update'] = abandoned_data['Last Updated Time Parsed'].apply(
        lambda x: (now - x).days if pd.notna(x) else None
    )
    
    abandoned_data = abandoned_data[abandoned_data['Days Since Update'].notna()]
    
    more_than_7 = abandoned_data[abandoned_data['Days Since Update'] > 7]
    more_than_15 = abandoned_data[abandoned_data['Days Since Update'] > 15]
    more_than_30 = abandoned_data[abandoned_data['Days Since Update'] > 30]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Not updated >7 days", len(more_than_7))
    with col2:
        st.metric("Not updated >15 days", len(more_than_15))
    with col3:
        st.metric("Not updated >30 days", len(more_than_30))
    
    st.markdown("---")
    
    categories = [
        ("More than 7 days (not updated since)", more_than_7, 7),
        ("More than 15 days (not updated since)", more_than_15, 15),
        ("More than 30 days (not updated since)", more_than_30, 30)
    ]
    
    for category_name, category_tickets, days_threshold in categories:
        st.subheader(f"🕒 {category_name}")
        
        if len(category_tickets) == 0:
            st.info(f"No tickets abandoned for more than {days_threshold} days")
            continue
        
        st.write(f"**Total tickets: {len(category_tickets)}**")
        
        category_tickets_sorted = category_tickets.sort_values('Days Since Update', ascending=False)
        
        with st.expander(f"➕ View all {len(category_tickets_sorted)} tickets", expanded=False):
            display_df = category_tickets_sorted[[
                'Request ID', 'Subject', 'Status.Name', 'Technician.Name', 
                'Days Since Update', 'Last Updated Time Parsed', 'Created Date Parsed'
            ]].copy()
            display_df['Last Updated'] = display_df['Last Updated Time Parsed'].apply(format_date_display)
            display_df['Created Date'] = display_df['Created Date Parsed'].apply(format_date_display)
            
            # Rename columns for display
            display_df = display_df.rename(columns={
                'Status.Name': 'Status',
                'Technician.Name': 'Technician'
            })
            
            display_df = display_df[[
                'Request ID', 'Subject', 'Status', 'Technician', 
                'Days Since Update', 'Last Updated', 'Created Date'
            ]]
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)


def main():
    st.set_page_config(
        page_title="Ticket Analysis Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Ticket Analysis Dashboard")
    st.markdown("Upload your ticket system CSV export to analyze metrics and generate email summaries.")
    
    # Ticket system selector
    st.sidebar.header("0️⃣ Ticket System Type")
    system_type = st.sidebar.radio(
        "Which ticket system are you using?",
        ["Manage Engine", "ConnectWise"],
        index=0
    )
    
    # Display required columns based on system type
    st.sidebar.markdown("---")
    st.sidebar.subheader("📋 Required Columns")
    
    with st.sidebar.expander(f"Click to see {system_type} columns", expanded=True):
        if system_type == "Manage Engine":
            st.write("**Manage Engine columns required:**")
            for i, col in enumerate(MANAGE_ENGINE_COLUMNS, 1):
                st.write(f"{i}. `{col}`")
        else:
            st.write("**ConnectWise columns required:**")
            for i, col in enumerate(CONNECTWISE_COLUMNS, 1):
                st.write(f"{i}. `{col}`")
    
    # File upload
    st.sidebar.markdown("---")
    st.sidebar.header("1️⃣ Upload CSV File")
    uploaded_file = st.sidebar.file_uploader(
        "Choose a CSV file",
        type=['csv'],
        help=f"Upload the CSV export from your {system_type} ticket system"
    )
    

    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            
            # Validate columns based on system type
            is_valid, missing = validate_csv(df, system_type)
            
            if not is_valid:
                st.error(f"❌ CSV Validation Failed!")
                st.markdown("### Missing Columns")
                st.write(f"Your file is missing **{len(missing)}** required column(s):")
                for col in missing:
                    st.write(f"- ❌ `{col}`")
                
                st.markdown("---")
                st.info(f"**Please ensure your {system_type} export includes all required columns listed in the sidebar.**")
                return
            
            # Convert ConnectWise to Manage Engine schema if needed
            if system_type == "ConnectWise":
                df = normalize_connectwise_to_manage_engine(df)
                st.info("🔄 ConnectWise data converted to internal processing format")
            
            # Prepare dataframe
            df = prepare_dataframe(df)
            
            st.success(f"✅ Successfully loaded {len(df)} tickets from {system_type}")
            st.markdown(f"**System:** {system_type} | **Total Records:** {len(df)}")
            
            # Date selectors
            st.sidebar.header("3️⃣ Select Comparison Dates")
            
            all_dates = pd.concat([
                df['Created Date Parsed'].dropna(),
                df['ClosedDT'].dropna()
            ])
            
            if len(all_dates) > 0:
                min_date = all_dates.min()
                max_date = all_dates.max()
                
                date_a = st.sidebar.date_input(
                    "Date A",
                    value=max_date,
                    min_value=min_date,
                    max_value=max_date
                )
                
                date_b = st.sidebar.date_input(
                    "Date B",
                    value=max_date,
                    min_value=min_date,
                    max_value=max_date
                )
            else:
                st.warning("⚠️ No valid dates found in the CSV")
                return
            
            # Breakdown scope selector
            st.sidebar.header("4️⃣ Breakdown Scope")
            scope_option = st.sidebar.radio(
                "Show breakdown for:",
                [
                    "Open tickets only",
                    "All tickets",
                    "Tickets created in period (Date A–Date B)",
                    "Tickets closed in period (Date A–Date B)"
                ],
                index=0
            )
            
            scope_map = {
                "Open tickets only": "open",
                "All tickets": "all",
                "Tickets created in period (Date A–Date B)": "created_in_period",
                "Tickets closed in period (Date A–Date B)": "closed_in_period"
            }
            scope_type = scope_map[scope_option]
            
            # ========== KEY METRICS ==========
            st.header("📈 Key Metrics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            open_tickets = get_open_tickets(df)
            closed_tickets = df[df['Is Closed']]
            
            with col1:
                st.metric("Total Open Tickets", len(open_tickets))
            with col2:
                opened_a = len(get_tickets_opened_on_date(df, date_a))
                st.metric(f"Opened on {format_date_display(date_a)}", opened_a)
            with col3:
                closed_a = len(get_tickets_closed_on_date(df, date_a))
                st.metric(f"Closed on {format_date_display(date_a)}", closed_a)
            with col4:
                st.metric("Total Closed Tickets", len(closed_tickets))
            
            # Current Year Metrics
            st.subheader(f"📅 Current Year ({datetime.now().year}) Metrics")
            
            year_metrics = get_current_year_metrics(df)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(f"Created in {year_metrics['year']}", year_metrics['created_total'])
            with col2:
                st.metric(f"Created {year_metrics['year']} - Currently Open", year_metrics['created_open'])
            with col3:
                st.metric(f"Created {year_metrics['year']} - Currently Closed", year_metrics['created_closed'])
            with col4:
                st.metric(f"Closed in {year_metrics['year']}", year_metrics['closed_total'])
            
            # ========== DATE COMPARISON ==========
            st.header("📅 Date Comparison")
            
            st.subheader("Specific Dates")
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Date A: {format_date_display(date_a)}**")
                opened_a = len(get_tickets_opened_on_date(df, date_a))
                closed_a = len(get_tickets_closed_on_date(df, date_a))
                st.write(f"Opened: {opened_a} tickets")
                st.write(f"Closed/Resolved: {closed_a} tickets")
            
            with col2:
                st.write(f"**Date B: {format_date_display(date_b)}**")
                opened_b = len(get_tickets_opened_on_date(df, date_b))
                closed_b = len(get_tickets_closed_on_date(df, date_b))
                st.write(f"Opened: {opened_b} tickets")
                st.write(f"Closed/Resolved: {closed_b} tickets")
            
            # Period metrics
            st.subheader(f"Period Metrics: {format_date_display(date_a)} to {format_date_display(date_b)} (Inclusive)")
            
            period_opened = df[
                (df['Created Date Parsed'] >= date_a) & 
                (df['Created Date Parsed'] <= date_b)
            ]
            
            period_closed = df[
                (df['Is Closed']) & 
                (df['ClosedDT'] >= date_a) & 
                (df['ClosedDT'] <= date_b)
            ]
            
            period_still_open = period_opened[~period_opened['Is Closed']]
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Opened in Period", len(period_opened))
                st.caption("Tickets created between Date A and Date B")
            with col2:
                st.metric("Closed/Resolved in Period", len(period_closed))
                st.caption("Closed tickets with ClosedDT in period")
            with col3:
                st.metric("Still Open from Period", len(period_still_open))
                st.caption("Created in period, currently open")
            
            # Rolling periods
            st.subheader("Rolling Periods")
            
            now = datetime.now()
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.write("**Last 24 Hours**")
                last_24h_opened = len(get_tickets_opened_since(df, now - timedelta(hours=24)))
                last_24h_closed = len(get_tickets_closed_since(df, now - timedelta(hours=24)))
                st.write(f"Opened: {last_24h_opened}")
                st.write(f"Closed/Resolved: {last_24h_closed}")
            
            with col2:
                st.write("**Last 7 Days**")
                last_7d_opened = len(get_tickets_opened_since(df, now - timedelta(days=7)))
                last_7d_closed = len(get_tickets_closed_since(df, now - timedelta(days=7)))
                st.write(f"Opened: {last_7d_opened}")
                st.write(f"Closed/Resolved: {last_7d_closed}")
            
            with col3:
                st.write("**Last 30 Days**")
                last_30d_opened = len(get_tickets_opened_since(df, now - timedelta(days=30)))
                last_30d_closed = len(get_tickets_closed_since(df, now - timedelta(days=30)))
                st.write(f"Opened: {last_30d_opened}")
                st.write(f"Closed/Resolved: {last_30d_closed}")
            

            # ========== TICKETS BREAKDOWN WITH DRILL-DOWN ==========
            st.header("🔍 Tickets Breakdown")
            st.info(f"📋 Current scope: **{scope_option}** | System: **{system_type}**")
            
            breakdown_tickets = get_tickets_in_period(df, date_a, date_b, scope_type)
            
            st.write(f"**Total tickets in scope: {len(breakdown_tickets)}**")
            
            tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
                "By Group",
                "By Sub-Category",
                "By IPC Feature",
                "By Technician",
                "By Requester",
                "By Priority",
                "By Status",
                "Tickets on DevOps",
                "Abandoned Tickets"
            ])
            
            with tab1:
                display_breakdown_with_drilldown(breakdown_tickets, 'Group.Name', 'Group')
            
            with tab2:
                display_breakdown_with_drilldown(breakdown_tickets, 'Sub Category.Name', 'Sub-Category')
            
            with tab3:
                display_breakdown_with_drilldown(breakdown_tickets, 'IPC Feature List', 'IPC Feature')
            
            with tab4:
                display_breakdown_with_drilldown(breakdown_tickets, 'Technician.Name', 'Technician')
            
            with tab5:
                display_breakdown_with_drilldown(breakdown_tickets, 'Requester.Name', 'Requester')
            
            with tab6:
                display_breakdown_with_drilldown(breakdown_tickets, 'Priority.Name', 'Priority')
            
            with tab7:
                display_breakdown_with_drilldown(breakdown_tickets, 'Status.Name', 'Status')
            
            with tab8:
                display_devops_breakdown(breakdown_tickets)
            
            with tab9:
                display_abandoned_tickets(breakdown_tickets)
            
            # ========== EMAIL SUMMARY ==========
            st.header("📧 Email-Ready Summary")
            
            summary_text = generate_email_summary(df, date_a, date_b)
            
            st.text_area(
                "Copy this summary to your email:",
                summary_text,
                height=500
            )
            
            # Download button
            st.download_button(
                label="💾 Download Summary as TXT",
                data=summary_text,
                file_name=f"ticket_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                mime="text/plain"
            )
            
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")
            st.exception(e)
    
    else:
        st.info("👈 Please upload a CSV file to begin analysis")


if __name__ == "__main__":
    main()
