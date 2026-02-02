import pandas as pd
import os

def split_csv_by_month(filename, at):
    # Load the dataset
    df = pd.read_csv(filename, parse_dates=[at])
    
    # Create an output directory so your main folder doesn't get cluttered
    output_dir = "split_files"
    os.makedirs(output_dir, exist_ok=True)
    
    # Group by year and month
    # %B = Full Month Name, %Y = 4-digit Year
    #for (year, month), group in df.groupby([df[at].dt.year, df[at].dt.month_name()]):
    for year, group in df.groupby(df[at].dt.year):
        
        # New naming convention: batch_Month_Year.csv
        #filename = f"batch_{month}_{year}.csv"
        filename = f"batch_{year}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Save the filtered data
        group.to_csv(filepath, index=False)
        print(f"Generated: {filepath}")

split_csv_by_month('amazon_reviews.csv', 'at')