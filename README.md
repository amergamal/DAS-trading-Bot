import logging
import os
import time
import pandas as pd
from datetime import datetime
from das_api_client import DasApiClient  # Ensure you import the real DasApiClient

# Configure logging to write to a file
log_file_path = os.path.join(os.getcwd(), 'real_time_ohlc_logging_test.log')
csv_file_path = os.path.join(os.getcwd(), 'ohlc_data.csv')
logging.basicConfig(
    level=logging.DEBUG,  # Set logging level to DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='w'),
        logging.StreamHandler()
    ]
)

class TMSDataHandler:
    def __init__(self, das_client):
        self.das_client = das_client
        self.tms_data = {}
        self.current_ohlc = {}
        self.csv_initialized = False
        print("Initialized TMSDataHandler")
        logging.debug("Initialized TMSDataHandler")

    def subscribe_to_tms(self, tickers):
        logging.info("Entering subscribe_to_tms")
        for ticker in tickers:
            try:
                subscribe_command = f'SB {ticker} tms'
                print(f'Sending subscribe command: {subscribe_command}')
                logging.debug(f'Sending subscribe command: {subscribe_command}')
                self.das_client.send_command(subscribe_command)
                response = self.das_client.receive_response()
                #print(f'Subscribe response for {ticker}: {response}')
                #logging.info(f'Subscribe response for {ticker}: {response}')
                if ticker not in self.tms_data:
                    self.tms_data[ticker] = pd.DataFrame(columns=['price', 'volume', 'timestamp', 'valid'])
                    self.current_ohlc[ticker] = {'open': None, 'high': None, 'low': None, 'close': None, 'volume': 0, 'minute': None}
            except Exception as e:
                print(f'Error subscribing to T&S for {ticker}: {e}')
                logging.error(f'Error subscribing to T&S for {ticker}: {e}', exc_info=True)
        logging.info("Exiting subscribe_to_tms")

    def update_ohlc(self, ticker, price, volume, timestamp):
        current_minute = timestamp.replace(second=0, microsecond=0)
        if ticker not in self.current_ohlc or self.current_ohlc[ticker]['minute'] != current_minute:
            # Log the completed OHLC for the previous minute
            print("parent1", current_minute)
            print(self.current_ohlc)
            if ticker in self.current_ohlc and self.current_ohlc[ticker]['minute'] is not None:
                self.log_ohlc(ticker, self.current_ohlc[ticker])
                self.write_ohlc_to_csv(ticker, self.current_ohlc[ticker])
                print("child1")

            # Initialize new OHLC for the current minute
            self.current_ohlc[ticker] = {'open': price, 'high': price, 'low': price, 'close': price, 'volume': volume, 'minute': current_minute}
        else:
            ohlc = self.current_ohlc[ticker]
            ohlc['high'] = max(ohlc['high'], price)
            ohlc['low'] = min(ohlc['low'], price)
            ohlc['close'] = price
            ohlc['volume'] += volume
            print("parent2")

    def log_ohlc(self, ticker, ohlc):
        logging.info(f"Completed OHLC for {ticker} - Minute: {ohlc['minute']}, Open: {ohlc['open']}, High: {ohlc['high']}, Low: {ohlc['low']}, Close: {ohlc['close']}, Volume: {ohlc['volume']}")

    def write_ohlc_to_csv(self, ticker, ohlc):
        print("CSV Creation")
        if not self.csv_initialized:
            print("pass initialize")
            try:
                with open(csv_file_path, 'w') as file:
                    print("CSV CREATED")
                    file.write("ticker,minute,open,high,low,close,volume\n")
                self.csv_initialized = True
            except Exception as e:
                print("failed to write error", e)

        with open(csv_file_path, 'a') as file:
            file.write(f"{ticker},{ohlc['minute']},{ohlc['open']},{ohlc['high']},{ohlc['low']},{ohlc['close']},{ohlc['volume']}\n")

    def handle_tms_data(self, ticker, data):
        logging.info("Entering handle_tms_data")
        try:
            # Print the raw data received
            #print(f'Raw data for {ticker}: {data}')
            #logging.debug(f'Raw data for {ticker}: {data}')

            parts = data.split()
            if len(parts) < 7:
                print('Invalid T&S data')
                logging.error('Invalid T&S data')
                return

            # Extract the condition field and check if bit 5 is set
            condition_str = parts[-1]
            try:
                condition = int(condition_str, 16)
            except ValueError:
                #print(f'Invalid condition format in T&S data: {condition_str}')
                #logging.error(f'Invalid condition format in T&S data: {condition_str}')
                return

            if (condition & 0x20) != 0:
                #print(f'Trade is not valid for {ticker} due to condition: {condition_str}')
               #logging.info(f'Trade is not valid for {ticker} due to condition: {condition_str}')
                pass

            # Simplified data parsing
            try:
                price = float(parts[2])
                volume = int(parts[3])
                timestamp_str = parts[5] if parts[4] in ('I', 'F', 'T', '@') else parts[4]
                timestamp = datetime.strptime(timestamp_str, '%H:%M:%S').replace(year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)

                #print(f'Parsed data - Ticker: {ticker}, Price: {price}, Volume: {volume}, Timestamp: {timestamp}')
                #logging.info(f'Parsed data - Ticker: {ticker}, Price: {price}, Volume: {volume}, Timestamp: {timestamp}')

                # Check if data is valid and log it
                if price > 0 and volume > 0:
                    #print(f'Valid T&S data for {ticker}: Price={price}, Volume={volume}, Timestamp={timestamp}')
                    #logging.debug(f'Valid T&S data for {ticker}: Price={price}, Volume={volume}, Timestamp={timestamp}')
                    self.update_ohlc(ticker, price, volume, timestamp)
                else:
                    print(f'Invalid T&S data for {ticker}: {data}')
                    #logging.error(f'Invalid T&S data for {ticker}: {data}')

            except ValueError as ve:
                print(f'ValueError while parsing T&S data for {ticker}: {data} - {ve}')
                logging.error(f'ValueError while parsing T&S data for {ticker}: {data} - {ve}', exc_info=True)
            except Exception as e:
                print(f'Error while parsing T&S data for {ticker}: {data} - {e}')
                logging.error(f'Error while parsing T&S data for {ticker}: {data} - {e}', exc_info=True)

        except Exception as e:
            print(f'Error handling T&S data for {ticker}: {e}')
            logging.error(f'Error handling T&S data for {ticker}: {e}', exc_info=True)
        logging.info("Exiting handle_tms_data")

    def handle_live_tms_data(self):
        logging.info("Entering handle_live_tms_data")
        print("Started handling live T&S data.")
        logging.info("Started handling live T&S data.")
        while True:
            try:
                response = self.das_client.receive_response()
                #print(f'Received response: {response}')
                #logging.debug(f'Received response: {response}')
                if response:
                    #print(f'Received live T&S data: {response}')
                    #logging.debug(f'Received live T&S data: {response}')
                    self.handle_tms_data('TSLA', response)
                else:
                    print('No data received from DAS API.')
                    #logging.debug('No data received from DAS API.')
            except Exception as e:
                print(f'Error while receiving T&S data: {e}')
                logging.error(f'Error while receiving T&S data: {e}', exc_info=True)
                time.sleep(1)  # Avoid busy loop if an error occurs
        logging.info("Exiting handle_live_tms_data")

print("Starting DAS client")
logging.info("Starting DAS client")
das_client = DasApiClient()
if das_client.login():
    print("DAS client logged in")
    logging.info("DAS client logged in")
    handler = TMSDataHandler(das_client)
    handler.subscribe_to_tms(['TSLA'])
    handler.handle_live_tms_data()
else:
    print("Failed to log in to DAS client")
    logging.error("Failed to log in to DAS client")

print(f"Logging is configured to write to '{log_file_path}'")
