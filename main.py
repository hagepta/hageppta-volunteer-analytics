#!/usr/bin/env python
import streamlit as st
from pathlib import Path
import gspread
import json
import os
from datetime import date, datetime # Ensure datetime is imported for current time
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage
import time
import pandas as pd  # Import the pandas library
import matplotlib.pyplot as plt
from google.cloud import storage
import io

def get_storage_client():
    """
    Initializes and returns a Google Cloud Storage client using the service account credentials.
    """
    try:
        # Load credentials from the service account key file
        creds_path = os.environ.get("GOOGLE_CREDS_PATH")
        creds_dir = Path(creds_path)
        creds_file = creds_dir / "hage-pta-fab6351c88f5.json"

        # Check if the credentials file exists
        if not creds_file.exists():
            print(f"Error: Google credentials file not found at {creds_file}.")
            return None

        # Create a client from the service account JSON key file
        client = storage.Client.from_service_account_json(creds_file)
        return client

    except Exception as e:
        print(f"Error initializing Google Cloud Storage client: {e}")
        return None

def save_plot_to_gcs(client, bucket_name, source_file_name, destination_blob_name):
    """
    Saves a plot (from a BytesIO object) to a Google Cloud Storage bucket.

    Args:
        client (google.cloud.storage.Client): The storage client.
        bucket_name (str): The ID of your GCS bucket.
        source_file_name (io.BytesIO): A BytesIO object containing the plot image data.
        destination_blob_name (str): The desired path/name of the file in the bucket.
    """
    if client is None:
        print("Storage client not initialized. Cannot save plot.")
        return

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # Rewind the stream to the beginning before uploading
        source_file_name.seek(0)
        
        # Upload the in-memory file
        blob.upload_from_file(source_file_name, content_type='image/png')
        print(f"File {destination_blob_name} uploaded to {bucket_name}.")
        
    except Exception as e:
        print(f"An error occurred while uploading the file: {e}")


def plot_sorted_total_hours_by_date(df):
    """
    Plots the total volunteer hours for each date, sorted from highest to lowest.

    Args:
        df (pd.DataFrame): A DataFrame containing volunteer hour data.
                          It must have 'submission_date' and 'hours' columns.
    """
    # Check if the necessary columns exist
    if 'submission_date' not in df.columns or 'hours' not in df.columns:
        print("Error: DataFrame must contain 'submission_date' and 'hours' columns.")
        return

    # Convert columns to the correct data types
    try:
        df['submission_date'] = pd.to_datetime(df['submission_date'])
        df['hours'] = pd.to_numeric(df['hours'])
    except Exception as e:
        print(f"Error converting data types: {e}")
        return

    # Group by date and sum the hours for each date
    daily_hours = df.groupby('submission_date')['hours'].sum().reset_index()

    # Sort the daily hours DataFrame by the 'hours' column in descending order
    sorted_daily_hours = daily_hours.sort_values(by='hours', ascending=False)

    # Create the plot
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(12, 7))

    # Create a bar plot
    plt.bar(sorted_daily_hours['submission_date'].dt.strftime('%m-%d'),
            sorted_daily_hours['hours'],
            color='teal',
            edgecolor='black')

    # Add titles and labels
    plt.title('Total Volunteer Hours by Date (Highest to Lowest)', fontsize=16, pad=20)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Total Hours', fontsize=12)

    # Rotate x-axis labels for better readability if dates are long
    plt.xticks(rotation=45, ha='right')

    # Add text labels on top of each bar to show the exact value
    for index, value in enumerate(sorted_daily_hours['hours']):
        plt.text(index, value + 0.5, str(value), ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    #save to a file
    output_file = 'sorted_total_hours_by_date.png'
    plt.savefig(output_file, dpi=300)
    plt.close()  # Close the plot to free up memory

def plot_cumulative_hours(df, bucket_name, client):
    """
    Plots the cumulative volunteer hours by date and saves it to a GCS bucket.

    Args:
        df (pd.DataFrame): DataFrame with 'submission_date' and 'hours' columns.
        bucket_name (str): The GCS bucket to save the plot to.
        client (google.cloud.storage.Client): The storage client.
    """
    if 'submission_date' not in df.columns or 'hours' not in df.columns:
        print("Error: DataFrame must contain 'submission_date' and 'hours' columns.")
        return

    try:
        df['submission_date'] = pd.to_datetime(df['submission_date'])
        df['hours'] = pd.to_numeric(df['hours'])
    except Exception as e:
        print(f"Error converting data types: {e}")
        return

    df.sort_values(by='submission_date', inplace=True)
    df['cumulative_hours'] = df['hours'].cumsum()

    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(10, 6))
    plt.plot(df['submission_date'], df['cumulative_hours'], marker='o', linestyle='-', color='b')
    plt.title('Cumulative PTA Volunteer Hours Over Time', fontsize=16, pad=20)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Cumulative Hours', fontsize=12)
    plt.gcf().autofmt_xdate()
    plt.grid(True)
    plt.tight_layout()

    # Save the plot to a BytesIO object in memory
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close() # Close the figure to free up memory

    # Save the plot from the in-memory object to Google Cloud Storage
    save_plot_to_gcs(client, bucket_name, buf, 'cumulative_hours_plot.png')

def plot_sorted_total_hours_by_date(df, bucket_name, client):
    """
    Plots the total volunteer hours for each date, sorted highest to lowest,
    and saves it to a GCS bucket.

    Args:
        df (pd.DataFrame): DataFrame with 'submission_date' and 'hours' columns.
        bucket_name (str): The GCS bucket to save the plot to.
        client (google.cloud.storage.Client): The storage client.
    """
    if 'submission_date' not in df.columns or 'hours' not in df.columns:
        print("Error: DataFrame must contain 'submission_date' and 'hours' columns.")
        return

    try:
        df['submission_date'] = pd.to_datetime(df['submission_date'])
        df['hours'] = pd.to_numeric(df['hours'])
    except Exception as e:
        print(f"Error converting data types: {e}")
        return

    daily_hours = df.groupby('submission_date')['hours'].sum().reset_index()
    sorted_daily_hours = daily_hours.sort_values(by='hours', ascending=False)

    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(12, 7))
    plt.bar(sorted_daily_hours['submission_date'].dt.strftime('%m-%d'),
            sorted_daily_hours['hours'],
            color='teal',
            edgecolor='black')
    plt.title('Total Volunteer Hours by Date (Highest to Lowest)', fontsize=16, pad=20)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Total Hours', fontsize=12)
    plt.xticks(rotation=45, ha='right')

    for index, value in enumerate(sorted_daily_hours['hours']):
        plt.text(index, value + 0.5, str(value), ha='center', va='bottom', fontsize=10)

    plt.tight_layout()

    # Save the plot to a BytesIO object in memory
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close() # Close the figure to free up memory

    # Save the plot from the in-memory object to Google Cloud Storage
    save_plot_to_gcs(client, bucket_name, buf, 'total_hours_plot.png')


def get_gsheet():
    # ... (Your existing get_gsheet function) ...
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_dict= None
    
    creds_path= os.environ.get("GOOGLE_CREDS_PATH")
    creds_dir= Path(creds_path)

    creds_file = creds_dir / "hage-pta-fab6351c88f5.json"

    try:
        with open(creds_file) as f:
            creds_dict = json.load(f)
    except FileNotFoundError:
        print("Google credentials file not found. Please check the path.")
        return None
    except json.JSONDecodeError:
        print("Error decoding JSON from Google credentials file.")
        return None

    if creds_dict:
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            client = gspread.authorize(creds)
            return client.open("PTA_Volunteer_Hours_2025-26").worksheet("hours")
        except Exception as e:
            print(f"Error authorizing Google Sheets: {e}")
            return None
        
def main():
    gsheet = get_gsheet()
    BUCKET_NAME = "volunteer_hours"
    
    if gsheet:
        try:
            # Get all records as a list of dictionaries
            data = gsheet.get_all_records()
            
            # Create a pandas DataFrame from the list of dictionaries
            df = pd.DataFrame(data)


            
            # You can now work with the DataFrame
            print("Successfully read Google Sheet into a pandas DataFrame.")

            storage_client = get_storage_client()
            if storage_client:
                # Call the plotting functions with the DataFrame, bucket name, and client
                plot_cumulative_hours(df, BUCKET_NAME, storage_client)
                plot_sorted_total_hours_by_date(df, BUCKET_NAME, storage_client)
            else:
                print("Skipping plot saving due to GCS client initialization error.")
            
            plot_cumulative_hours(df, BUCKET_NAME, storage_client)
            plot_sorted_total_hours_by_date(df, BUCKET_NAME, storage_client)
            
            
            
        except gspread.exceptions.APIError as e:
            print(f"API Error when reading from Google Sheet: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None
    else:
        print("Could not connect to Google Sheet.")
        return None

if __name__ == "__main__":
    main()