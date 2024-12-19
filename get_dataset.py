import yfinance as yf
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import math

global isWriteHeader
isWriteHeader = True


def fetch_ticker_data(comp_code):
    try:
        # for comp_code in range(600000, 6000011):
        fetch_obj = yf.Ticker(f"{comp_code}.SS")
        blc_sheet = fetch_obj.get_balance_sheet(
            as_dict=True, pretty=True, freq="yearly"
        )
        imc_stm = fetch_obj.get_income_stmt(as_dict=True, pretty=True, freq="yearly")
        return blc_sheet, imc_stm
    except Exception as e:
        return f"Error: {e}"


def write_to_csv(financial_data, comp_code):
    global isWriteHeader
    headers = set()
    for date, data in financial_data.items():
        headers.update(data.keys())
    headers = ["Company Code", "Date"] + sorted(headers)

    for date, data in financial_data.items():
        date_str = str(date)  # Convert date to string
        year = date_str.split("-")[0]  # Extract year from the date
        output_file = f"fin_data_{year}.csv"
        with open(output_file, newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            first_row = next(
                reader, None
            )  # Get the first row or None if the file is empty

            if first_row is None:
                isWriteHeader = True
            else:
                isWriteHeader = False
        with open(output_file, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            if isWriteHeader:
                writer.writerow(headers)  # Write headers
                isWriteHeader = False
            row = [comp_code, date] + [
                (
                    data.get(key, None)
                    if not (
                        isinstance(data.get(key), float) and math.isnan(data.get(key))
                    )
                    else "NaN"
                )
                for key in headers[2:]
            ]
            writer.writerow(row)

        print(f"Data for {year} successfully written to {output_file}")


if __name__ == "__main__":

    for comp_code in range(600001, 604000):
        blc_sheet, icm_stm = fetch_ticker_data(comp_code)
        write_to_csv(icm_stm, comp_code)
        time.sleep(2)
