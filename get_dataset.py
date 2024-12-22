import yfinance as yf
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import math
import pandas

global isWriteHeader
isWriteHeader = True


def fetch_ticker_data(comp_code):
    try:
        # for comp_code in range(600000, 6000011):
        fetch_obj = yf.Ticker(f"{comp_code}.SS")
        # blc_sheet = fetch_obj.get_balance_sheet(
        #     as_dict=True, pretty=True, freq="yearly"
        # )
        get_indicators(fetch_obj)
    except Exception as e:
        return f"Error: {e}"


def get_indicators(ticker: yf.Ticker):
    try:
        income_stmt = ticker.financials
        years = income_stmt.columns  # Get years in fin stm
        balance_sheet = ticker.balance_sheet
        for year in years:

            net_income = income_stmt.loc["Net Income", year]  # Net Income - worked
            share_outstanding = ticker.info["sharesOutstanding"]  # Share Outstanding

            basic_EPS = income_stmt.loc["Basic EPS", year]  # EPS

            total_stock_equity = balance_sheet.loc["Stockholders Equity"]
            bvps = total_stock_equity[year] / share_outstanding  # BVPS

            total_assets = balance_sheet.loc["Total Assets"]
            ROA = net_income / total_assets[year]  # ROA

            total_equity = balance_sheet.loc["Total Equity Gross Minority Interest"]
            ROE = total_assets[year] / total_equity[year]  # ROE

            dividends = ticker.dividends
            dividends_by_year = dividends.resample("YE").sum()
            dividends_by_year.index = dividends_by_year.index.year  # DIV

            historical_data = ticker.history(period="max")
            year_end_prices = historical_data["Close"].resample("YE").last()
            year_end_prices.index = year_end_prices.index.year
            pe_ratio = year_end_prices[year.year] / basic_EPS  # P/E Ratio

            total_debt = balance_sheet.loc["Total Debt"]
            DAR = total_debt[year] / total_assets[year]  # DAR

            MB = year_end_prices[year.year] / bvps

            print(f"\nYear: {year.year}")
            print(f"Net Income: {net_income:.2f}")
            print(f"Share Outstanding: {share_outstanding:.2f}")
            print(f"EPS: {basic_EPS}")
            print(f"BVPS: {bvps:.2f}")
            print(f"ROA: {ROA:.2f}")
            print(f"ROE: {ROE:.2f}")
            print(f"DIV: {dividends_by_year[year.year]:.2f}")
            print(f"P/E Ratio {pe_ratio:.2f}")
            print(f"Closed price: {year_end_prices[year.year]:.2f}")
            print(f"DAR: {DAR:.2f}")
            print(f"MB: {MB:.2f}")

    except Exception as e:
        print(f"Idicator error: {e}")


# def write_to_csv(financial_data, comp_code):
#     global isWriteHeader
#         headers = set()
#         for date, data in financial_data.items():
#             headers.update(data.keys())
#         headers = ["Company Code", "Date"] + sorted(headers)

#         for date, data in financial_data.items():
#             date_str = str(date)  # Convert date to string
#             year = date_str.split("-")[0]  # Extract year from the date
#             output_file = f"fin_data_{year}.csv"
#             with open(output_file, newline="", encoding="utf-8") as csvfile:
#             reader = csv.reader(csvfile)
#             first_row = next(
#                 reader, None
#             )  # Get the first row or None if the file is empty

#             if first_row is None:
#                 isWriteHeader = True
#             else:
#                 isWriteHeader = False
#         with open(output_file, mode="a", newline="", encoding="utf-8") as file:
#             writer = csv.writer(file)
#             if isWriteHeader:
#                 writer.writerow(headers)  # Write headers
#                 isWriteHeader = False
#             row = [comp_code, date] + [
#                 (
#                     data.get(key, None)
#                     if not (
#                         isinstance(data.get(key), float) and math.isnan(data.get(key))
#                     )
#                     else "NaN"
#                 )
#                 for key in headers[2:]
#             ]
#             writer.writerow(row)

#         print(f"Data for {year} successfully written to {output_file}")


if __name__ == "__main__":
    fetch_obj = yf.Ticker("600000.SS")
    get_indicators(fetch_obj)
    print("Done")
    # for comp_code in range(600001, 600003):
    #     fetch_ticker_data
    # blc_sheet, icm_stm = fetch_ticker_data(comp_code)
    # write_to_csv(icm_stm, comp_code)
    # time.sleep(2)
