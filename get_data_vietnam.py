from vnstock3 import Vnstock
from vnstock3.explorer.vci import Finance
import pandas as pd
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

isFetch = False


def fetch_indicator(comp_code, start_from):
    try:
        if comp_code == start_from:
            global isFetch
            isFetch = True
        if isFetch:
            stock = Vnstock().stock(symbol=comp_code, source="VCI")
            company = Vnstock().stock(symbol=comp_code, source="TCBS").company

            icm_stm = stock.finance.income_statement(period="year", lang="en")
            balanced_sheet = stock.finance.balance_sheet(period="quarter")
            finance = stock.finance.ratio(period="quarter", lang="en")
            years = finance["Meta", "yearReport"]
            for year in years.unique():
                if year > 2019:
                    for quarter in range(1, 5):
                        EPS = finance.loc[
                            (finance["Meta", "yearReport"] == year)
                            & (finance["Meta", "lengthReport"] == quarter),
                            ("Chỉ tiêu định giá", "EPS (VND)"),
                        ].values
                        EPS = EPS[0] if len(EPS) != 0 else pd.NA
                        BVPS = finance.loc[
                            (finance["Meta", "yearReport"] == year)
                            & (finance["Meta", "lengthReport"] == quarter),
                            ("Chỉ tiêu định giá", "BVPS (VND)"),
                        ].values
                        BVPS = BVPS[0] if len(BVPS) != 0 else pd.NA
                        ROA = finance.loc[
                            (finance["Meta", "yearReport"] == year)
                            & (finance["Meta", "lengthReport"] == quarter),
                            ("Chỉ tiêu khả năng sinh lợi", "ROA (%)"),
                        ].values
                        ROA = ROA[0] if len(ROA) != 0 else pd.NA
                        ROE = finance.loc[
                            (finance["Meta", "yearReport"] == year)
                            & (finance["Meta", "lengthReport"] == quarter),
                            ("Chỉ tiêu khả năng sinh lợi", "ROE (%)"),
                        ].values
                        ROE = ROE[0] if len(ROE) != 0 else pd.NA
                        DY = finance.loc[
                            (finance["Meta", "yearReport"] == year)
                            & (finance["Meta", "lengthReport"] == quarter),
                            ("Chỉ tiêu khả năng sinh lợi", "Dividend yield (%)"),
                        ].values
                        DY = DY[0] if len(DY) != 0 else pd.NA

                        PE = finance.loc[
                            (finance["Meta", "yearReport"] == year)
                            & (finance["Meta", "lengthReport"] == quarter),
                            ("Chỉ tiêu định giá", "P/E"),
                        ].values
                        PE = PE[0] if len(PE) != 0 else pd.NA
                        Market_Cap = finance.loc[
                            (finance["Meta", "yearReport"] == year)
                            & (finance["Meta", "lengthReport"] == quarter),
                            ("Chỉ tiêu định giá", "Market Capital (Bn. VND)"),
                        ].values
                        Market_Cap = Market_Cap[0] if len(Market_Cap) != 0 else pd.NA
                        Total_Asset = balanced_sheet.loc[
                            (finance["Meta", "yearReport"] == year)
                            & (finance["Meta", "lengthReport"] == quarter),
                            "TOTAL ASSETS (Bn. VND)",
                        ].values
                        Total_Asset = Total_Asset[0] if len(Total_Asset) != 0 else pd.NA
                        PB = finance.loc[
                            (finance["Meta", "yearReport"] == year)
                            & (finance["Meta", "lengthReport"] == quarter),
                            ("Chỉ tiêu định giá", "P/B"),
                        ].values
                        PB = PB[0] if len(PB) != 0 else pd.NA
                        Total_Debt = balanced_sheet.loc[
                            (finance["Meta", "yearReport"] == year)
                            & (finance["Meta", "lengthReport"] == quarter),
                            "LIABILITIES (Bn. VND)",
                        ].values
                        Total_Debt = Total_Debt[0] if len(Total_Debt) != 0 else pd.NA
                        DAR = Total_Debt / Total_Asset
                        if quarter != 4:
                            year_end_prices = (
                                stock.quote.history(
                                    start=f"{year}-0{3*quarter}-01",
                                    end=f"{year}-0{3*quarter}-30",
                                    interval="1D",
                                )
                                .tail(1)["close"]
                                .values[0]
                            )
                        else:
                            year_end_prices = (
                                stock.quote.history(
                                    start=f"{year}-{3*quarter}-01",
                                    end=f"{year}-{3*quarter}-31",
                                    interval="1D",
                                )
                                .tail(1)["close"]
                                .values[0]
                            )
                        DIV = year_end_prices * DY
                        comp_name = company.overview()["short_name"].values[0]
                        industry = company.overview()["industry"].values[0]

                        print(f"\nYear: {year}")
                        print(f"Quarter: {quarter}")
                        print(f"Company Name: {comp_name}")
                        print(f"Industry: {industry}")
                        print(f"DIV: {DIV}")
                        print(f"EPS: {EPS}")
                        print(f"BVPS: {BVPS}")
                        print(f"ROA: {ROA}")
                        print(f"ROE: {ROE}")
                        print(f"P/E Ratio {PE}")
                        print(f"Closed price: {year_end_prices}")
                        print(f"DAR: {DAR}")
                        print(f"MB: {PB}")
                        print(f"Market Cap: {Market_Cap}")
                        print(f"DY: {DY}")

                        fin_data = {
                            "Company code": comp_code,
                            "Company Name": comp_name,
                            "Industry": industry,
                            "Year": year,
                            "Quarter": quarter,
                            "EPS": f"{EPS}",
                            "BVPS": f"{BVPS}",
                            "ROA": f"{ROA}",
                            "ROE": f"{ROE}",
                            "P/E Ratio": f"{PE}",
                            "DAR": f"{DAR}",
                            "MB": f"{PB}",
                            "DY": f"{DY}",
                            "DIV": f"{DIV}",
                            "Market Cap": f"{Market_Cap}",
                            "Total Assets": f"{Total_Asset}",
                            "Stock Price": f"{year_end_prices}",
                        }
                        write_to_csv(fin_data)

    except Exception as e:
        print(f"Idicator error: {e}")


isWriteHeader = False


def test():
    stock = Vnstock().stock(symbol="FPT", source="VCI")
    company = Vnstock().stock(symbol="FPT", source="TCBS").company
    icm_stm = stock.finance.income_statement(period="quarter", lang="en")
    balanced_sheet = stock.finance.balance_sheet(period="quarter")
    finance = stock.finance.ratio(period="quarter", lang="en")
    years = finance["Meta", "yearReport"]
    print(years)
    for year in years.unique():
        if year > 2019:
            for quarter in range(1, 5):
                EPS = finance.loc[
                    (finance["Meta", "yearReport"] == year)
                    & (finance["Meta", "lengthReport"] == quarter),
                    ("Chỉ tiêu định giá", "EPS (VND)"),
                ].values
                EPS = EPS[0] if len(EPS) != 0 else pd.NA
                print(quarter)
                print(EPS)


def write_to_csv(fin_data):
    output_file = f"vietnam_data.csv"
    with open(output_file, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fin_data.keys())
        if isWriteHeader:
            writer.writeheader()  # Write the header row (column names)
        writer.writerow(fin_data)

    print(f"Data for {fin_data["Year"]} successfully written to {output_file}")


if __name__ == "__main__":
    isFetch = True
    stock = Vnstock().stock(source="VCI")
    stock_list = stock.listing.symbols_by_group("HNX")
    for stock in stock_list.values:
        fetch_indicator(stock, "AAV")
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(fetch_indicator, stock)
    # test()
