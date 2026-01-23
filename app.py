import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

# Configuration
REQUIRED_COLUMNS = [
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

# MANDATORY: Normalized closed statuses (lowercase)
CLOSED_STATUSES = {'closed', 'resolved'}
ON_HOLD_STATUSES = {"Defered Enhancement", "In Progress", "Tll BAU Ticket", "BA Triage Required", "Devops Assigned", "BAU Config Change", "On Hold"}

def validate_csv(df):
    """
    Validate that all required columns are present in the dataframe.
    Returns (is_valid, missing_columns)
    """
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    return len(missing) == 0, missing


def parse_date_column(series):
    """
    Parse a date column, handling various formats and returning date only (no time).
    Returns a Series of datetime.date objects or NaT.
    """
    # Try multiple date formats
    parsed = pd.to_datetime(series, errors='coerce', dayfirst=False)
    # Convert to date only (strip time component)
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
    result = result.replace('nan', 'Unassigned')  # After string conversion
    return result


def prepare_dataframe(df):
    """
    Prepare the dataframe by:
    - Normalizing status (mandatory lowercase strip)
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
    
    # Determine if ticket is closed (status must be in closed set)
    df['Is Closed'] = df['Status Clean'].isin(CLOSED_STATUSES)
    
    # Calculate effective closed date (ClosedDT logic)
    # ClosedDT = Completed Time if present, else Last Updated Time
    # Only valid for closed tickets
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
    """
    Get all open tickets (status is NOT in closed set).
    """
    return df[~df['Is Closed']].copy()


def get_tickets_opened_on_date(df, target_date):
    """
    Get tickets that were opened on a specific date.
    """
    return df[df['Created Date Parsed'] == target_date].copy()


def get_tickets_closed_on_date(df, target_date):
    """
    Get tickets that were closed on a specific date.
    Only includes tickets with closed/resolved status.
    """
    return df[(df['Is Closed']) & (df['ClosedDT'] == target_date)].copy()


def get_tickets_opened_since(df, cutoff_datetime):
    """
    Get tickets opened since cutoff (Created Date >= cutoff).
    """
    cutoff_date = cutoff_datetime.date()
    return df[df['Created Date Parsed'] >= cutoff_date].copy()


def get_tickets_closed_since(df, cutoff_datetime):
    """
    Get tickets closed since cutoff (Is Closed AND ClosedDT >= cutoff).
    """
    cutoff_date = cutoff_datetime.date()
    return df[(df['Is Closed']) & (df['ClosedDT'] >= cutoff_date)].copy()


def get_tickets_in_period(df, date_a, date_b, scope_type):
    """
    Get tickets based on scope type and period (Date A to Date B inclusive).
    
    scope_type can be:
    - 'open': Open tickets only
    - 'all': All tickets
    - 'created_in_period': Tickets created between Date A and Date B
    - 'closed_in_period': Tickets closed between Date A and Date B
    """
    if scope_type == 'open':
        return get_open_tickets(df)
    
    elif scope_type == 'all':
        return df.copy()
    
    elif scope_type == 'created_in_period':
        # Tickets created between Date A and Date B (inclusive)
        return df[
            (df['Created Date Parsed'] >= date_a) & 
            (df['Created Date Parsed'] <= date_b)
        ].copy()
    
    elif scope_type == 'closed_in_period':
        # Closed/Resolved tickets with ClosedDT between Date A and Date B (inclusive)
        return df[
            (df['Is Closed']) & 
            (df['ClosedDT'] >= date_a) & 
            (df['ClosedDT'] <= date_b)
        ].copy()
    
    else:
        return df.copy()


def count_by_column(df, column_name):
    """
    Count tickets grouped by a specific column.
    Returns a sorted dataframe with counts.
    Does NOT filter out Unassigned - includes all values.
    """
    if len(df) == 0:
        return pd.DataFrame(columns=[column_name, 'Count'])
    
    counts = df[column_name].value_counts().reset_index()
    counts.columns = [column_name, 'Count']
    
    return counts.sort_values('Count', ascending=False)


def get_current_year_metrics(df):
    """
    Calculate metrics for the current year.
    Returns dict with year metrics.
    """
    current_year = datetime.now().year
    
    # Tickets created in current year
    created_this_year = df[df['Created Date Parsed'].apply(
        lambda x: x.year == current_year if pd.notna(x) else False
    )].copy()
    
    # Of those created this year, how many are open vs closed
    created_year_open = created_this_year[~created_this_year['Is Closed']]
    created_year_closed = created_this_year[created_this_year['Is Closed']]
    
    # Tickets closed in current year (regardless of when created)
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
    """
    Get daily opened vs closed counts for the last N days.
    Returns a dataframe with Date, Opened, Closed columns.
    """
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days-1)
    
    # Create date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    trend_data = []
    for date in date_range:
        date_only = date.date()
        
        # Opened on this date
        opened = len(df[df['Created Date Parsed'] == date_only])
        
        # Closed on this date (must be closed status)
        closed = len(df[(df['Is Closed']) & (df['ClosedDT'] == date_only)])
        
        trend_data.append({
            'Date': date_only,
            'Opened': opened,
            'Closed': closed
        })
    
    return pd.DataFrame(trend_data)


def generate_email_summary(df, date_a, date_b):
    """
    Generate an email-ready summary of ticket metrics.
    All dates formatted as dd/mm/yyyy.
    """
    now = datetime.now()
    
    # Calculate metrics
    open_tickets = get_open_tickets(df)
    total_open = len(open_tickets)
    
    # Date A and B metrics (date-only)
    opened_a = len(get_tickets_opened_on_date(df, date_a))
    closed_a = len(get_tickets_closed_on_date(df, date_a))
    opened_b = len(get_tickets_opened_on_date(df, date_b))
    closed_b = len(get_tickets_closed_on_date(df, date_b))
    
    # Rolling period metrics
    last_24h_opened = len(get_tickets_opened_since(df, now - timedelta(hours=24)))
    last_24h_closed = len(get_tickets_closed_since(df, now - timedelta(hours=24)))
    
    last_7d_opened = len(get_tickets_opened_since(df, now - timedelta(days=7)))
    last_7d_closed = len(get_tickets_closed_since(df, now - timedelta(days=7)))
    
    last_30d_opened = len(get_tickets_opened_since(df, now - timedelta(days=30)))
    last_30d_closed = len(get_tickets_closed_since(df, now - timedelta(days=30)))
    
    # Breakdowns
    by_group = count_by_column(open_tickets, 'Group.Name')
    by_subcategory = count_by_column(open_tickets, 'Sub Category.Name')
    by_ipc = count_by_column(open_tickets, 'IPC Feature List')
    by_technician = count_by_column(open_tickets, 'Technician.Name')
    
    # Format dates as dd/mm/yyyy
    date_a_str = format_date_display(date_a)
    date_b_str = format_date_display(date_b)
    now_str = now.strftime('%d/%m/%Y at %I:%M %p')
    
    # Build summary text
def generate_email_summary(df, date_a, date_b):
    now = datetime.now()
    now_str = now.strftime('%d/%m/%Y at %H:%M')

    # ---------- helper to build tables ----------
    def build_table(title, scope_df):
        status_series = scope_df["Status.Name"].astype(str).str.strip()
        status_clean = scope_df["Status Clean"].astype(str).str.strip().str.lower()

        created_count = len(scope_df)

        # SAME definition used twice (as requested)
        on_hold_count = len(scope_df[status_series.isin(ON_HOLD_STATUSES)])
        pending_action_count = len(scope_df[status_series.isin(ON_HOLD_STATUSES)])

        pending_user_update_count = len(scope_df[status_series.eq("Pending User Update")])

        closed_count = len(scope_df[status_clean.isin(CLOSED_STATUSES)])

        table = f"""### {title}

| Metric | Count |
|---|---:|
| Tickets created | {created_count} |
| Tickets On Hold | {on_hold_count} |
| Tickets Pending Action (TLL/Business) | {pending_action_count} |
| Tickets Pending User Update | {pending_user_update_count} |
| Tickets Closed/Resolved | {closed_count} |
"""
        return table

    # ---------- scopes ----------
    # All tickets
    all_scope = df.copy()

    # Selected period (created in Date A–Date B)
    period_scope = df[
        (df["Created Date Parsed"] >= date_a) &
        (df["Created Date Parsed"] <= date_b)
    ].copy()

    # Current month (created this month)
    today = datetime.now().date()
    month_start = today.replace(day=1)
    month_scope = df[
        (df["Created Date Parsed"] >= month_start) &
        (df["Created Date Parsed"] <= today)
    ].copy()

    # ---------- build summary ----------
    summary = f"""TICKET SYSTEM SUMMARY
Generated on: {now_str}

"""

    summary += build_table("Overview – All Tickets", all_scope)
    summary += build_table(
        f"Selected Period – Created {format_date_display(date_a)} to {format_date_display(date_b)}",
        period_scope
    )
    summary += build_table(
        f"Current Month – Created {format_date_display(month_start)} to {format_date_display(today)}",
        month_scope
    )

    return summary

    # Add group breakdown
    if len(by_group) > 0:
        for _, row in by_group.iterrows():
            summary += f"• {row['Group.Name']}: {row['Count']} tickets\n"
    else:
        summary += "• No open tickets\n"
    
    summary += "\nBy Sub-Category:\n"
    if len(by_subcategory) > 0:
        for _, row in by_subcategory.head(20).iterrows():
            summary += f"• {row['Sub Category.Name']}: {row['Count']} tickets\n"
        if len(by_subcategory) > 20:
            summary += f"• ... and {len(by_subcategory) - 10} more categories\n"
    else:
        summary += "• No open tickets\n"
    
    summary += "\nBy IPC Feature:\n"
    if len(by_ipc) > 0:
        for _, row in by_ipc.head(20).iterrows():
            summary += f"• {row['IPC Feature List']}: {row['Count']} tickets\n"
        if len(by_ipc) > 20:
            summary += f"• ... and {len(by_ipc) - 10} more features\n"
    else:
        summary += "• No open tickets\n"
    
    summary += "\nBy Technician:\n"
    if len(by_technician) > 0:
        for _, row in by_technician.iterrows():
            summary += f"• {row['Technician.Name']}: {row['Count']} tickets\n"
    else:
        summary += "• No open tickets\n"
    
    
    
    return summary


def display_breakdown_with_drilldown(tickets_df, column_name, label):
    """
    Display a breakdown with expandable drill-down for each group.
    Shows count summary, then expanders with ticket details.
    """
    counts = count_by_column(tickets_df, column_name)
    
    if len(counts) == 0:
        st.info(f"No tickets in selected scope")
        return
    
    st.write(f"**Total groups: {len(counts)}**")
    
    for _, row in counts.iterrows():
        group_name = row[column_name]
        count = row['Count']
        
        # Create expander for each group
        with st.expander(f"➕ {group_name} ({count})"):
            # Get tickets for this group
            group_tickets = tickets_df[tickets_df[column_name] == group_name]
            
            # Display ticket details with extended columns
            display_df = group_tickets[[
                'Request ID', 'Subject', 'Status.Name', 'Group.Name', 
                'Requester.Name', 'Technician.Name', 'Created Date Parsed', 'Completed Time Parsed'
            ]].copy()
            
            display_df['Created Date'] = display_df['Created Date Parsed'].apply(format_date_display)
            display_df['Completed Time'] = display_df['Completed Time Parsed'].apply(format_date_display)
            display_df = display_df[[
                'Request ID', 'Subject', 'Status.Name', 'Group.Name', 
                'Requester.Name', 'Technician.Name', 'Created Date', 'Completed Time'
            ]]
            # Rename columns for display only (UI-friendly headers)
            display_df = display_df.rename(columns={
                "Request ID": "Ticket Number",
                "Subject": "Subject",
                "Status.Name": "Status",
                "Group.Name": "Group",
                "Requester.Name": "Requester",
                "Technician.Name": "Technician",
                "Created Date": "Created Date",
                "Completed Time": "Completed Date"
})

            st.dataframe(display_df, use_container_width=True, hide_index=True)


def display_devops_breakdown(tickets_df):
    """
    Display DevOps breakdown with drill-down.
    Shows DevOpsRef, Request ID, and Subject in expanded view.
    """
    # Checkbox to filter only real DevOpsRef values
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
        
        # Create expander for each DevOpsRef
        with st.expander(f"➕ {devops_ref} ({count})"):
            # Get tickets for this DevOpsRef
            group_tickets = filtered_tickets[filtered_tickets['DevOpsRef'] == devops_ref]
            
            # Display ticket details with DevOpsRef and extended columns
            display_df = group_tickets[[
                'DevOpsRef', 'Request ID', 'Subject', 'Status.Name', 'Group.Name', 
                'Requester.Name', 'Technician.Name', 'Created Date Parsed', 'Completed Time Parsed'
            ]].copy()
            
            display_df['Created Date'] = display_df['Created Date Parsed'].apply(format_date_display)
            display_df['Completed Time'] = display_df['Completed Time Parsed'].apply(format_date_display)
            display_df = display_df[[
                'DevOpsRef', 'Request ID', 'Subject', 'Status.Name', 'Group.Name', 
                'Requester.Name', 'Technician.Name', 'Created Date', 'Completed Time'
            ]]
        # Rename columns for display only (UI-friendly headers)
            display_df = display_df.rename(columns={
               "DevOpsRef": "DevOps Ticket Number",
               "Request ID": "ME Number",
               "Subject": "Subject",
               "Status.Name": "Status",
               "Group.Name": "Group",
               "Requester.Name": "Requester",
               "Technician.Name": "Technician",
               "Created Date": "Created Date",
               "Completed Time": "Completed Date"
})

            st.dataframe(display_df, use_container_width=True, hide_index=True)


def display_abandoned_tickets(tickets_df):
    """
    Display abandoned tickets breakdown based on days since last update.
    Categories: >7 days, >15 days, >30 days
    Works with whatever scope is selected.
    """
    now = datetime.now().date()
    
    # Calculate days since last update for each ticket
    abandoned_data = tickets_df.copy()
    abandoned_data['Days Since Update'] = abandoned_data['Last Updated Time Parsed'].apply(
        lambda x: (now - x).days if pd.notna(x) else None
    )
    
    # Filter out tickets with no last update date
    abandoned_data = abandoned_data[abandoned_data['Days Since Update'].notna()]
    
    # Categorize tickets
    more_than_7 = abandoned_data[abandoned_data['Days Since Update'] > 7]
    more_than_15 = abandoned_data[abandoned_data['Days Since Update'] > 15]
    more_than_30 = abandoned_data[abandoned_data['Days Since Update'] > 30]
    
    # Display summary metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Not updated >7 days", len(more_than_7))
    
    with col2:
        st.metric("Not updated >15 days", len(more_than_15))
    
    with col3:
        st.metric("Not updated >30 days", len(more_than_30))
    
    st.markdown("---")
    
    # Display each category with drill-down
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
        
        # Sort by days since update (oldest first)
        category_tickets_sorted = category_tickets.sort_values('Days Since Update', ascending=False)
        
        # Single expander showing all tickets in this category
        with st.expander(f"➕ View all {len(category_tickets_sorted)} tickets", expanded=False):
            # Display ticket details with days since update and last updated date
            display_df = category_tickets_sorted[[
                'Request ID', 'Subject', 'Status.Name', 'Technician.Name', 
                'Days Since Update', 'Last Updated Time Parsed', 'Created Date Parsed'
            ]].copy()
            display_df['Last Updated'] = display_df['Last Updated Time Parsed'].apply(format_date_display)
            display_df['Created Date'] = display_df['Created Date Parsed'].apply(format_date_display)
            display_df = display_df[[
                'Request ID', 'Subject', 'Status.Name', 'Technician.Name', 
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
    
    # File upload
    st.sidebar.header("1️⃣ Upload CSV File")
    uploaded_file = st.sidebar.file_uploader(
        "Choose a CSV file",
        type=['csv'],
        help="Upload the CSV export from your ticket system"
    )
    
    # Optional: Show charts checkbox
    st.sidebar.header("2️⃣ Display Options")
    show_charts = st.sidebar.checkbox("Show charts", value=True)
    
    if uploaded_file is not None:
        try:
            # Read CSV
            df = pd.read_csv(uploaded_file)
            
            # Validate columns
            is_valid, missing = validate_csv(df)
            
            if not is_valid:
                st.error(f"❌ Missing required columns: {', '.join(missing)}")
                st.info("Required columns: " + ", ".join(REQUIRED_COLUMNS))
                return
            
            # Prepare dataframe
            df = prepare_dataframe(df)
            
            st.success(f"✅ Successfully loaded {len(df)} tickets")
            
            # Date selectors
            st.sidebar.header("3️⃣ Select Comparison Dates")
            
            # Get min and max dates from the data
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
            
            # Map scope option to scope type
            scope_map = {
                "Open tickets only": "open",
                "All tickets": "all",
                "Tickets created in period (Date A–Date B)": "created_in_period",
                "Tickets closed in period (Date A–Date B)": "closed_in_period"
            }
            scope_type = scope_map[scope_option]
            
            # ========== KEY METRICS ==========
            st.header("📈 Key Metrics")
            
            # Basic metrics
            col1, col2, col3, col4, col5  = st.columns(5)
            
            open_tickets = get_open_tickets(df)
            closed_tickets = df[df['Is Closed']]
            status_series = df["Status.Name"].astype(str).str.strip()
            on_hold = df[status_series.isin(ON_HOLD_STATUSES)]
            in_progress = df[status_series.eq("In Progress")]
            pending_user_update = df[status_series.eq("Pending User Update")]
            with col1:
                st.metric("Total Open Tickets", len(open_tickets))
            
            with col2:
                st.metric("Tickets On Hold (Pending Business/TLL Action)", len(on_hold))
            
            with col3:
                st.metric("Tickets In Progress", len(in_progress))
            
            with col4:
                st.metric("Tickets Pending User Update", len(pending_user_update))
            
            with col5:
                st.metric("Total Closed Tickets", len(closed_tickets))
            # Current Year Metrics
            st.subheader(f"📅 Current Year ({datetime.now().year}) Metrics")
            
            year_metrics = get_current_year_metrics(df)
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            current_year = year_metrics["year"]

            # tickets created in the current year (mesma base do get_current_year_metrics)
            tickets_year = df[df["Created Date Parsed"].apply(
               lambda x: x.year == current_year if pd.notna(x) else False
            )]

            status_year = tickets_year["Status.Name"].astype(str).str.strip()

            year_open = tickets_year[~tickets_year["Is Closed"]]
            year_closed = tickets_year[tickets_year["Is Closed"]]

            year_on_hold = tickets_year[status_year.isin(ON_HOLD_STATUSES)]
            year_pending_user = tickets_year[status_year.eq("Pending User Update")]
            year_in_progress = tickets_year[status_year.eq("In Progress")]

            with col1:
                st.metric("Open Tickets (Year)", len(year_open))

            with col2:
                st.metric("Pending Business / TLL Action", len(year_on_hold))

            with col3:
                st.metric("Pending User Update", len(year_pending_user))

            with col4:
                st.metric("In Progress", len(year_in_progress))

            with col5:
                st.metric("Closed Tickets (Year)", len(year_closed))

            
            # ========== DATE COMPARISON ==========
            st.header("📅 Date Comparison")
            
            # # Specific dates
            # st.subheader("Specific Dates")
            # col1, col2 = st.columns(2)
            
            # with col1:
                # st.write(f"**Date A: {format_date_display(date_a)}**")
                # opened_a = len(get_tickets_opened_on_date(df, date_a))
                # closed_a = len(get_tickets_closed_on_date(df, date_a))
                # st.write(f"Opened: {opened_a} tickets")
                # st.write(f"Closed/Resolved: {closed_a} tickets")
            
            # with col2:
                # st.write(f"**Date B: {format_date_display(date_b)}**")
                # opened_b = len(get_tickets_opened_on_date(df, date_b))
                # closed_b = len(get_tickets_closed_on_date(df, date_b))
                # st.write(f"Opened: {opened_b} tickets")
                # st.write(f"Closed/Resolved: {closed_b} tickets")
            
            # Period metrics (Date A to Date B inclusive)
            st.subheader(f"Period Metrics: {format_date_display(date_a)} to {format_date_display(date_b)} (Inclusive)")
            
            # Calculate period metrics
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
                last_7d_closed = len(get_tickets_closed_since(df, now - timedelta (days=7)))
                st.write(f"Opened: {last_7d_opened}")
                st.write(f"Closed/Resolved: {last_7d_closed}")

            with col3:
                st.write("**Last 30 Days**")
                last_30d_opened = len(get_tickets_opened_since(df, now - timedelta(days=30)))
                last_30d_closed = len(get_tickets_closed_since(df, now - timedelta(days=30)))
                st.write(f"Opened: {last_30d_opened}")
                st.write(f"Closed/Resolved: {last_30d_closed}")
        
        # ========== CHARTS (OPTIONAL) ==========
            if show_charts:
                st.header("📊 Charts")
            
                # Get tickets based on selected scope for charts
                chart_tickets = get_tickets_in_period(df, date_a, date_b, scope_type)
            
            # Bar charts for tickets in scope
            # st.subheader(f"Ticket Distribution (Top 10) - {scope_option}")
            
            # tab1, tab2, tab3, tab4 = st.tabs([
                # "By Group",
                # "By Technician",
                # "By Sub-Category",
                # "By Status"
            # ])
            
            # with tab1:
                # by_group = count_by_column(chart_tickets, 'Group.Name').head(10)
                # if len(by_group) > 0:
                    # chart_df = by_group.set_index('Group.Name')
                    # st.bar_chart(chart_df['Count'])
                # else:
                    # st.info("No tickets in selected scope")
            
            # with tab2:
                # by_tech = count_by_column(chart_tickets, 'Technician.Name').head(10)
                # if len(by_tech) > 0:
                    # chart_df = by_tech.set_index('Technician.Name')
                    # st.bar_chart(chart_df['Count'])
                # else:
                    # st.info("No tickets in selected scope")
            
            # with tab3:
                # by_subcat = count_by_column(chart_tickets, 'Sub Category.Name').head(10)
                # if len(by_subcat) > 0:
                    # chart_df = by_subcat.set_index('Sub Category.Name')
                    # st.bar_chart(chart_df['Count'])
                # else:
                    # st.info("No tickets in selected scope")
            
            # with tab4:
                # by_status = count_by_column(chart_tickets, 'Status.Name').head(10)
                # if len(by_status) > 0:
                    # chart_df = by_status.set_index('Status.Name')
                    # st.bar_chart(chart_df['Count'])
                # else:
                    # st.info("No tickets in selected scope")
            
                # Trend line chart
                st.subheader("Daily Trend: Opened vs Closed (Last 30 Days)")
            
                trend_data = get_daily_trend_data(df, days=30)
            
                if len(trend_data) > 0:
                # Prepare for line chart
                    chart_df = trend_data.set_index('Date')
                    st.line_chart(chart_df)
                else:
                    st.info("No trend data available")
        
        # ========== OPEN TICKETS BREAKDOWN WITH DRILL-DOWN ==========
            st.header("🔍 Tickets Breakdown")
            st.info(f"📋 Current scope: **{scope_option}**")
        
        # Get tickets based on selected scope
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
                abandoned_scope = breakdown_tickets[
                ~breakdown_tickets["Status Clean"].isin({"closed", "resolved", "canceled"})
                ]
                display_abandoned_tickets(abandoned_scope)
            
          # ========== EMAIL Main ==========
            def generate_email_summary(df, date_a, date_b):
                today = datetime.now().date()
                month_start = today.replace(day=1)

                # Snapshot date from CSV (best available proxy)
                snapshot_date = df["Last Updated Time Parsed"].dropna().max()
                snapshot_str = format_date_display(snapshot_date) if pd.notna(snapshot_date) else "N/A"

                def make_metrics_df(scope_df, closed_scope_df=None):
                    status_series = scope_df["Status.Name"].astype(str).str.strip()


                    created_count = len(scope_df)

                    on_hold_count = len(scope_df[status_series.isin(ON_HOLD_STATUSES)])
                    pending_action_count = len(scope_df[status_series.isin(ON_HOLD_STATUSES)])

                    pending_user_update_count = len(scope_df[status_series.eq("Pending User Update")])
                     # ✅ IMPORTANT: Closed/Resolved should come from CLOSED scope (ClosedDT in range)
                    if closed_scope_df is None:
                       closed_count = int(scope_df["Is Closed"].sum())  # fallback
                    else:
                        closed_count = len(closed_scope_df)

                    return pd.DataFrame([
                        {"Metric": "🆕 Tickets created", "Count": created_count},
                        {"Metric": "⏸️ Tickets On Hold", "Count": on_hold_count},
                        {"Metric": "🕒 Pending Action (TLL/Business)", "Count": pending_action_count},
                        {"Metric": "🙋 Pending User Update", "Count": pending_user_update_count},
                        {"Metric": "✅ Closed/Resolved", "Count": closed_count},
                    ])

                overview_df = make_metrics_df(df)

                period_scope = df[
                    (df["Created Date Parsed"] >= date_a) &
                    (df["Created Date Parsed"] <= date_b)
                ].copy()
                # ✅ Closed/Resolved in PERIOD (by ClosedDT) - same as Date Comparison
                period_closed_scope = df[
                    (df["Is Closed"]) &
                    (df["ClosedDT"] >= date_a) &
                    (df["ClosedDT"] <= date_b)
                ].copy()
                
                period_df = make_metrics_df(period_scope, closed_scope_df=period_closed_scope)

                month_scope = df[
                    (df["Created Date Parsed"] >= month_start) &
                    (df["Created Date Parsed"] <= today)
                ].copy()
                
                # ✅ Closed/Resolved in MONTH (by ClosedDT)
                month_closed_scope = df[
                    (df["Is Closed"]) &
                    (df["ClosedDT"] >= month_start) &
                    (df["ClosedDT"] <= today)
                ].copy()
                month_df = make_metrics_df(month_scope)

                def df_to_plain(title, metrics_df):
                    lines = [f"{title}"]
                    for _, row in metrics_df.iterrows():
                        lines.append(f"- {row['Metric']}: {row['Count']}")
                    return "\n".join(lines)

                plain_text = (
                    "TICKETS SUMMARY\n"
                    f"CSV Snapshot Date: {snapshot_str}\n\n"
                    + df_to_plain("📌 Overview (All Tickets)", overview_df)
                    + "\n\n"
                    + df_to_plain(
                        f"📆 Selected Period ( {format_date_display(date_a)} → {format_date_display(date_b)})",
                        period_df
                    )
                    + "\n\n"
                    + df_to_plain(
                        f"🗓️ Current Month ( {format_date_display(month_start)} → {format_date_display(today)})",
                        month_df
                    )
                )

                def df_to_html(title, metrics_df):
                    html_table = metrics_df.to_html(index=False, border=0)
                    return f"""
            <h3 style="margin:14px 0 8px 0;">{title}</h3>
            {html_table}
            """

                html = f"""
            <div style="font-family:Segoe UI, Arial, sans-serif; font-size:13px; color:#222;">
              <h2 style="margin:0 0 6px 0;">Tickets Summary</h2>
              <div style="margin:0 0 14px 0;"><b>CSV Snapshot Date:</b> {snapshot_str}</div>

              {df_to_html("📌 Overview (All Tickets)", overview_df)}
              {df_to_html(f"📆 Selected Period ( {format_date_display(date_a)} → {format_date_display(date_b)})", period_df)}
              {df_to_html(f"🗓️ Current Month ( {format_date_display(month_start)} → {format_date_display(today)})", month_df)}
            </div>
            """

                return {
                    "snapshot_str": snapshot_str,
                    "overview_df": overview_df,
                    "period_df": period_df,
                    "month_df": month_df,
                    "month_start": month_start,
                    "today": today,
                    "plain_text": plain_text,
                    "html": html.strip()
                }

        # ========== EMAIL SUMMARY ==========
            st.header("📧 Email-Ready Summary")

            summary = generate_email_summary(df, date_a, date_b)

            st.subheader("📌 Overview (All Tickets)")
            st.dataframe(summary["overview_df"], use_container_width=True, hide_index=True)

            st.subheader(f"📆 Selected Period ( {format_date_display(date_a)} → {format_date_display(date_b)})")
            st.dataframe(summary["period_df"], use_container_width=True, hide_index=True)

            st.subheader(
                f"🗓️ Current Month ( {format_date_display(summary['month_start'])} → {format_date_display(summary['today'])})"
            )
            st.dataframe(summary["month_df"], use_container_width=True, hide_index=True)

            st.text_area("Copy this into your email:", summary["plain_text"], height=260)

            st.download_button(
                label="💾 Download Summary as TXT",
                data=summary["plain_text"],
                file_name=f"ticket_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                mime="text/plain"
            )

        except Exception as e:
               st.error(f"❌ Error processing file: {str(e)}")
               st.exception(e)

        else:
               st.info("👈 Please upload a CSV file to begin analysis")
    
    # Show example of required columns
    with st.expander("📋 Required CSV Columns"):
        st.write("Your CSV must contain these columns:")
        for col in REQUIRED_COLUMNS:
            st.write(f"• {col}")
            
if __name__ == "__main__":
    main()