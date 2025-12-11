# laptop_client.py - READ FROM EXCEL AND SEND TO PI (NO RESPONSE NEEDED)

import socket
import json
import time
import pandas as pd

HOST = '172.20.10.2'  # Pi's IP
PORT = 5000

def send_sensor_data_from_excel(row):
    """Send sensor data from Excel row to Pi"""
    client_socket = None
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(5)
        client_socket.connect((HOST, PORT))
        
        # Convert Status from text to integer
        status_text = str(row.get('Status', '')).strip().lower()
        if status_text == 'charging':
            status_value = 1
        else:  # 'discharging' or anything else
            status_value = 0
        
        # Convert Excel row to sensor data format
        sensor_data = {
            'Voltage': float(row.get('Voltage', 0)),
            'Impedance': float(row.get('Impedance', 0)),
            'IntTemp': float(row.get('IntTemp', 0)),
            'SurfaceTemp': float(row.get('SurfaceTemp', 0)),
            'Capacity': float(row.get('Capacity', 0)),
            'SoC': float(row.get('SoC', 0)),
            'Status': status_value  # Use converted integer
        }
        
        print(f"Sending data: {sensor_data}")
        
        # Send data to Pi and immediately close
        client_socket.send(json.dumps(sensor_data).encode('utf-8'))
        
        print("Data sent successfully")
        return True
        
    except Exception as e:
        print(f"Connection error: {e}")
        return False
    finally:
        # Close socket if it was created
        if client_socket:
            client_socket.close()

# READ EXCEL FILE
print("Loading Excel file...")
try:
    df = pd.read_excel(r"C:\Users\Avaryn\Downloads\unexpected_running_conditions.xlsx")
    
    # Remove if file doesn't have error column
    df = df.drop(columns=['expected error'], errors='ignore')
    
    # Force numeric types
    for col in ['Voltage', 'Impedance', 'IntTemp', 'SurfaceTemp', 'Capacity', 'SoC']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    print(f"Loaded {len(df)} rows from Excel file")
    
except Exception as e:
    print(f"Error loading Excel file: {e}")
    exit(1)

# MAIN LOOP - Send each row from Excel
print(f"Starting laptop client - sending data to {HOST}:{PORT}")
print("Press Ctrl+C to stop")

row_index = 0

try:
    while row_index < len(df):
        row = df.iloc[row_index]
        row_index += 1
        
        print(f"\nSending row {row_index}/{len(df)}")
        print("-" * 50)
        
        send_sensor_data_from_excel(row)
        
        # Wait 1 second before sending next row
        print("Waiting 1 second...")
        time.sleep(1)
        
    print("\nAll Excel data sent!")
    
except KeyboardInterrupt:
    print("\nStopped by user")
    
except Exception as e:
    print(f"Unexpected error: {e}")
