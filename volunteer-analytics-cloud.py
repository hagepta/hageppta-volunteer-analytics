import pandas as pd
import gspread
import json
import io
import matplotlib.pyplot as plt
from pathlib import Path
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage
import functions_framework

@functions_framework.http
def generate_and_save_plots(request):
    """
    HTTP Cloud Function/Run service that generates and saves plots to a GCS bucket.
    """
    # --- Configuration ---
    BUCKET_NAME = "volunteer_hours"
    
    # Path where the secret will be mounted in the container.
    # The name of the file inside the container is 'credentials.json'.
    CREDS_FILE_PATH = "/etc/secrets/credentials.json"
    
    try:
        # Step 1: Authorize Google Sheets with the credentials file from the mounted secret
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE_PATH, scope)
            client = gspread.authorize(creds)
            gsheet = client.open("PTA_Volunteer_Hours_2025-26").worksheet("hours")
        except Exception as e:
            print(f"Error authorizing Google Sheets: {e}")
            return f"Error authorizing Google Sheets: {e}", 500

        # Step 2: Read the data into a pandas DataFrame
        data = gsheet.get_all_records()
        hours_df = pd.DataFrame(data)
        print("Google Sheet data successfully loaded into DataFrame.")

        # Step 3: Initialize Google Cloud Storage client.
        storage_client = storage.Client()

        # Step 4: Generate and save the plots to the GCS bucket
        print("Generating and saving plots...")
        plot_cumulative_hours(hours_df, BUCKET_NAME, storage_client)
        plot_sorted_total_hours_by_date(hours_df, BUCKET_NAME, storage_client)
        
        return "Plots generated and saved successfully!", 200
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return f"An unexpected error occurred: {e}", 500

# The following helper functions are unchanged from the previous code block.
def save_plot_to_gcs(client, bucket_name, source_file_name, destination_blob_name):
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        source_file_name.seek(0)
        blob.upload_from_file(source_file_name, content_type='image/png')
        print(f"File {destination_blob_name} uploaded to {bucket_name}.")
    except Exception as e:
        print(f"An error occurred while uploading the file: {e}")

def plot_cumulative_hours(df, bucket_name, client):
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
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    save_plot_to_gcs(client, bucket_name, buf, 'cumulative_hours_plot.png')

def plot_sorted_total_hours_by_date(df, bucket_name, client):
    """
    Plots the total volunteer hours for each date, sorted from highest to lowest.
    """
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
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    save_plot_to_gcs(client, bucket_name, buf, 'total_hours_plot.png')


#gcloud builds submit --tag gcr.io/hage-pta/pta-analytics-job --region us-central1
#gcloud run deploy pta-analytics-job \    --image gcr.io/hage-pta/pta-analytics-job \
#    --region us-central1 \
#    --allow-unauthenticated \
#    --set-secrets=/etc/secrets/credentials.json=volunteer_analytics:latest